﻿// -----------------------------------------------------------------------
//  <copyright file="BaseByteArrayJournalDao.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Journal.Types;
using Akka.Persistence.Sql.Serialization;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util.Internal;
using LanguageExt;
using LinqToDB;
using LinqToDB.Data;
using static LanguageExt.Prelude;
using Array = System.Array;

namespace Akka.Persistence.Sql.Journal.Dao
{
    public static class SequentialUuidGenerator
    {
        private static readonly AtomicCounterLong Counter = new(DateTime.UtcNow.Ticks);

        /// <summary>
        ///     Gets a value to be assigned to a property.
        /// </summary>
        /// <param name="entry">The change tracking entry of the entity for which the value is being generated.</param>
        /// <returns>The value to be assigned to a property.</returns>
        public static Guid Next()
        {
            var guidBytes = Guid.NewGuid().ToByteArray();
            var counterBytes = BitConverter.GetBytes(Counter.GetAndIncrement());

            if (!BitConverter.IsLittleEndian)
            {
                Array.Reverse(counterBytes);
            }

            guidBytes[08] = counterBytes[1];
            guidBytes[09] = counterBytes[0];
            guidBytes[10] = counterBytes[7];
            guidBytes[11] = counterBytes[6];
            guidBytes[12] = counterBytes[5];
            guidBytes[13] = counterBytes[4];
            guidBytes[14] = counterBytes[3];
            guidBytes[15] = counterBytes[2];

            return new Guid(guidBytes);
        }
    }

    public abstract class BaseByteArrayJournalDao : BaseJournalDaoWithReadMessages, IJournalDaoWithUpdates
    {
        protected static readonly Expression<Func<JournalMetaData, PersistenceIdAndSequenceNumber>> MetaDataSelector =
            md => new PersistenceIdAndSequenceNumber(md.SequenceNumber, md.PersistenceId);

        protected static readonly Expression<Func<JournalRow, PersistenceIdAndSequenceNumber>> RowDataSelector =
            md => new PersistenceIdAndSequenceNumber(md.SequenceNumber, md.PersistenceId);

        private static readonly Expression<Func<PersistenceIdAndSequenceNumber, long>> SequenceNumberSelector =
            r => r.SequenceNumber;

        private readonly Flow<JournalRow, Util.Try<ReplayCompletion>, NotUsed> _deserializeFlowMapped;
        private readonly TagMode _tagWriteMode;
        protected readonly JournalConfig JournalConfig;

        protected readonly ILoggingAdapter Logger;
        protected readonly FlowPersistentReprSerializer<JournalRow> Serializer;
        public readonly ISourceQueueWithComplete<WriteQueueEntry> WriteQueue;

        protected BaseByteArrayJournalDao(
            IAdvancedScheduler scheduler,
            IMaterializer materializer,
            AkkaPersistenceDataConnectionFactory connectionFactory,
            JournalConfig config,
            ByteArrayJournalSerializer serializer,
            ILoggingAdapter logger)
            : base(scheduler, materializer, connectionFactory)
        {
            Logger = logger;
            JournalConfig = config;
            Serializer = serializer;
            _deserializeFlowMapped = Serializer.DeserializeFlow().Select(MessageWithBatchMapper());
            _tagWriteMode = JournalConfig.PluginConfig.TagMode;

            // Due to C# rules we have to initialize WriteQueue here
            // Keeping it here vs init function prevents accidental moving of init
            // to where variables aren't set yet.
            WriteQueue = Source
                .Queue<WriteQueueEntry>(JournalConfig.DaoConfig.BufferSize, OverflowStrategy.DropNew)
                .BatchWeighted(
                    JournalConfig.DaoConfig.BatchSize,
                    cf => cf.Rows.Count,
                    r => new WriteQueueSet(ImmutableList.Create(new[] { r.Tcs }), r.Rows),
                    (oldRows, newRows) =>
                        new WriteQueueSet(
                            oldRows.Tcs.Add(newRows.Tcs),
                            oldRows.Rows.Concat(newRows.Rows)))
                .SelectAsync(
                    JournalConfig.DaoConfig.Parallelism,
                    async promisesAndRows =>
                    {
                        try
                        {
                            await WriteJournalRows(promisesAndRows.Rows);
                            foreach (var taskCompletionSource in promisesAndRows.Tcs)
                                taskCompletionSource.TrySetResult(NotUsed.Instance);
                        }
                        catch (Exception e)
                        {
                            foreach (var taskCompletionSource in promisesAndRows.Tcs)
                                taskCompletionSource.TrySetException(e);
                        }

                        return NotUsed.Instance;
                    })
                .ToMaterialized(
                    Sink.Ignore<NotUsed>(),
                    Keep.Left).Run(Materializer);
        }

        public async Task<IImmutableList<Exception>> AsyncWriteMessages(
            IEnumerable<AtomicWrite> messages,
            long timeStamp = 0)
        {
            var serializedTries = Serializer.Serialize(messages, timeStamp);

            // Fold our List of Lists into a single sequence
            var rows = Seq(FlattenListOfListsToList(serializedTries));

            // Wait for the write to go through. If Task fails, write will be captured as WriteMessagesFailure.
            await QueueWriteJournalRows(rows);

            // If we get here, we build an ImmutableList containing our rejections.
            // These will be captured as WriteMessagesRejected
            return BuildWriteRejections(serializedTries);
        }

        public async Task Delete(string persistenceId, long maxSequenceNr)
        {
            await using var connection = ConnectionFactory.GetConnection();

            var transaction = await connection.BeginTransactionAsync();

            try
            {
                await connection
                    .GetTable<JournalRow>()
                    .Where(r =>
                        r.PersistenceId == persistenceId &&
                        r.SequenceNumber <= maxSequenceNr)
                    .Set(r => r.Deleted, true)
                    .UpdateAsync();

                var maxMarkedDeletion =
                    await MaxMarkedForDeletionMaxPersistenceIdQuery(connection, persistenceId).FirstOrDefaultAsync();

                if (JournalConfig.DaoConfig.SqlCommonCompatibilityMode)
                {
                    await connection.GetTable<JournalMetaData>()
                        .InsertOrUpdateAsync(
                            insertSetter: () => new JournalMetaData
                            {
                                PersistenceId = persistenceId,
                                SequenceNumber = maxMarkedDeletion
                            },
                            onDuplicateKeyUpdateSetter: jmd => new JournalMetaData(),
                            keySelector: () => new JournalMetaData
                            {
                                PersistenceId = persistenceId,
                                SequenceNumber = maxMarkedDeletion
                            });
                }

                await connection
                    .GetTable<JournalRow>()
                    .Where(r =>
                        r.PersistenceId == persistenceId &&
                        r.SequenceNumber <= maxSequenceNr &&
                        r.SequenceNumber < maxMarkedDeletion)
                    .DeleteAsync();

                if (JournalConfig.DaoConfig.SqlCommonCompatibilityMode)
                {
                    await connection
                        .GetTable<JournalMetaData>()
                        .Where(r =>
                            r.PersistenceId == persistenceId &&
                            r.SequenceNumber < maxMarkedDeletion)
                        .DeleteAsync();
                }

                if (JournalConfig.PluginConfig.TagMode != TagMode.Csv)
                {
                    await connection
                        .GetTable<JournalTagRow>()
                        .Where(r =>
                            r.SequenceNumber <= maxSequenceNr &&
                            r.PersistenceId == persistenceId)
                        .DeleteAsync();
                }

                await transaction.CommitAsync();
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "Error on delete!");

                try
                {
                    await transaction.RollbackAsync();
                }
                catch (Exception exception)
                {
                    throw new AggregateException(exception, ex);
                }

                throw;
            }
        }

        public async Task<Done> Update(string persistenceId, long sequenceNr, object payload)
        {
            var write = new Persistent(payload, sequenceNr, persistenceId);

            var serialize = Serializer.Serialize(write);

            if (serialize.IsSuccess)
            {
                throw new ArgumentException(
                    $"Failed to serialize {write.GetType()} for update of {persistenceId}] @ {sequenceNr}",
                    serialize.Failure.Value);
            }

            await using var connection = ConnectionFactory.GetConnection();

            await connection
                .GetTable<JournalRow>()
                .Where(
                    r =>
                        r.PersistenceId == persistenceId &&
                        r.SequenceNumber == write.SequenceNr)
                .Set(r => r.Message, serialize.Get().Message)
                .UpdateAsync();

            return Done.Instance;
        }

        public async Task<long> HighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            await using var connection = ConnectionFactory.GetConnection();

            return (await MaxSeqNumberForPersistenceIdQuery(connection, persistenceId, fromSequenceNr).MaxAsync())
                .GetValueOrDefault(0);
        }

        /// <summary>
        ///     This override is greedy since it is always called
        ///     from within <see cref="BaseJournalDaoWithReadMessages.MessagesWithBatch" />
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="persistenceId"></param>
        /// <param name="fromSequenceNr"></param>
        /// <param name="toSequenceNr"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public override Source<Util.Try<ReplayCompletion>, NotUsed> Messages(
            DataConnection connection,
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max)
        {
            IQueryable<JournalRow> query = connection
                .GetTable<JournalRow>()
                .Where(r =>
                    r.PersistenceId == persistenceId &&
                    r.SequenceNumber >= fromSequenceNr &&
                    r.SequenceNumber <= toSequenceNr &&
                    r.Deleted == false)
                .OrderBy(r => r.SequenceNumber);

            if (max <= int.MaxValue)
                query = query.Take((int)max);

            return Source
                .FromTask(query.ToListAsync())
                .SelectMany(r => r)
                .Via(_deserializeFlowMapped);
        }

        private async Task QueueWriteJournalRows(Seq<JournalRow> xs)
        {
            var promise = new TaskCompletionSource<NotUsed>(TaskCreationOptions.RunContinuationsAsynchronously);

            // Send promise and rows into queue. If the Queue takes it,
            // It will write the Promise state when finished writing (or failing)
            var result = await WriteQueue.OfferAsync(new WriteQueueEntry(promise, xs));

            switch (result)
            {
                case QueueOfferResult.Enqueued:
                    break;

                case QueueOfferResult.Failure f:
                    promise.TrySetException(new Exception("Failed to write journal row batch", f.Cause));
                    break;

                case QueueOfferResult.Dropped:
                    promise.TrySetException(
                        new Exception(
                            $"Failed to enqueue journal row batch write, the queue buffer was full ({JournalConfig.DaoConfig.BufferSize} elements)"));
                    break;

                case QueueOfferResult.QueueClosed:
                    promise.TrySetException(
                        new Exception(
                            "Failed to enqueue journal row batch write, the queue was closed."));
                    break;
            }

            await promise.Task;
        }

        private async Task WriteJournalRows(Seq<JournalRow> xs)
        {
            switch (xs.Count)
            {
                case 0:
                    break;

                // hot path:
                // If we only have one row, penalty for BulkCopy
                // Isn't worth it due to insert caching/transaction/etc.
                case 1 when _tagWriteMode == TagMode.Csv || xs.Head().TagArr.Length == 0:
                {
                    // If we are writing a single row,
                    // we don't need to worry about transactions.
                    await using var connection = ConnectionFactory.GetConnection();
                    await connection.InsertAsync(xs.Head);
                    break;
                }

                default:
                    await InsertMultiple(xs);
                    break;
            }
        }

        private async Task InsertMultiple(Seq<JournalRow> xs)
        {
            await using var connection = ConnectionFactory.GetConnection();

            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);

            try
            {
                if (_tagWriteMode == TagMode.Csv)
                {
                    await BulkInsertNoTagTableTags(connection, xs, JournalConfig.DaoConfig);
                }
                else
                {
                    var config = JournalConfig.DaoConfig;
                    var tail = xs;
                    while (tail.Count > 0)
                    {
                        (var noTags, tail) = tail.Span(r => r.TagArr.Length == 0);
                        if (noTags.Count > 0)
                            await BulkInsertNoTagTableTags(connection, noTags, config);

                        (var hasTags, tail) = tail.Span(r => r.TagArr.Length > 0);
                        if (hasTags.Count > 0)
                            await InsertWithOrderingAndBulkInsertTags(connection, hasTags, config);
                    }
                }

                await connection.CommitTransactionAsync();
            }
            catch (Exception e1)
            {
                try
                {
                    await connection.RollbackTransactionAsync();
                }
                catch (Exception e2)
                {
                    throw new AggregateException(e2, e1);
                }

                throw;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static async Task InsertWithOrderingAndBulkInsertTags(
            DataConnection connection,
            Seq<JournalRow> xs,
            BaseByteArrayJournalDaoConfig config)
        {
            var tagsToInsert = new List<JournalTagRow>(xs.Count);

            // We could not do bulk copy and retrieve the inserted ids
            // Issue: https://github.com/linq2db/linq2db/issues/2960
            // We're forced to insert the rows one by one.
            foreach (var journalRow in xs)
            {
                var dbId = await connection.InsertWithInt64IdentityAsync(journalRow);

                tagsToInsert.AddRange(
                    journalRow.TagArr.Select(
                        s1 => new JournalTagRow
                        {
                            OrderingId = dbId,
                            TagValue = s1,
                            PersistenceId = journalRow.PersistenceId,
                            SequenceNumber = journalRow.SequenceNumber
                        }));
            }

            await connection
                .GetTable<JournalTagRow>()
                .BulkCopyAsync(
                    new BulkCopyOptions
                    {
                        BulkCopyType = BulkCopyType.MultipleRows,
                        UseParameters = config.PreferParametersOnMultiRowInsert,
                        MaxBatchSize = config.DbRoundTripTagBatchSize
                    },
                    tagsToInsert);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static async Task BulkInsertNoTagTableTags(
            DataConnection connection,
            Seq<JournalRow> xs,
            BaseByteArrayJournalDaoConfig config)
            => await connection
                .GetTable<JournalRow>()
                .BulkCopyAsync(
                    new BulkCopyOptions
                    {
                        BulkCopyType = xs.Count > config.MaxRowByRowSize
                            ? BulkCopyType.Default
                            : BulkCopyType.MultipleRows,
                        UseParameters = config.PreferParametersOnMultiRowInsert,
                        MaxBatchSize = config.DbRoundTripBatchSize
                    },
                    xs);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static Guid NextUuid()
            => SequentialUuidGenerator.Next();

        // By using a custom flatten here, we avoid an Enumerable/LINQ allocation
        // And are able to have a little more control over default capacity of array.
        private static List<JournalRow> FlattenListOfListsToList(List<Util.Try<JournalRow[]>> source)
        {
            var rows = new List<JournalRow>(
                source.Count > 4
                    ? source.Count
                    : 4);

            foreach (var t in source)
            {
                var item = t.Success.Value;
                if (item is { })
                    rows.AddRange(item);
            }

            return rows;
        }

        protected static ImmutableList<Exception> BuildWriteRejections(List<Util.Try<JournalRow[]>> serializedTries)
        {
            var builderEx = new Exception[serializedTries.Count];
            for (var i = 0; i < serializedTries.Count; i++)
                builderEx[i] = serializedTries[i].Failure.Value;

            return ImmutableList.CreateRange(builderEx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static IQueryable<long> MaxMarkedForDeletionMaxPersistenceIdQuery(
            DataConnection connection,
            string persistenceId)
            => connection
                .GetTable<JournalRow>()
                .Where(r => r.PersistenceId == persistenceId && r.Deleted)
                .OrderByDescending(r => r.SequenceNumber)
                .Select(r => r.SequenceNumber)
                .Take(1);

        private IQueryable<long?> MaxSeqNumberForPersistenceIdQuery(
            DataConnection connection,
            string persistenceId,
            long minSequenceNumber = 0)
        {
            if (minSequenceNumber != 0)
            {
                return JournalConfig.DaoConfig.SqlCommonCompatibilityMode
                    ? MaxSeqForPersistenceIdQueryableCompatibilityModeWithMinId(
                        connection,
                        persistenceId,
                        minSequenceNumber)
                    : MaxSeqForPersistenceIdQueryableNativeModeMinId(
                        connection,
                        persistenceId,
                        minSequenceNumber);
            }

            return JournalConfig.DaoConfig.SqlCommonCompatibilityMode
                ? MaxSeqForPersistenceIdQueryableCompatibilityMode(connection, persistenceId)
                : MaxSeqForPersistenceIdQueryableNativeMode(connection, persistenceId);
        }

        private static IQueryable<long?> MaxSeqForPersistenceIdQueryableNativeMode(
            DataConnection connection,
            string persistenceId)
            => connection
                .GetTable<JournalRow>()
                .Where(r => r.PersistenceId == persistenceId)
                .Select(r => (long?)r.SequenceNumber);

        private static IQueryable<long?> MaxSeqForPersistenceIdQueryableNativeModeMinId(
            DataConnection connection,
            string persistenceId,
            long minSequenceNumber)
            => connection
                .GetTable<JournalRow>()
                .Where(r =>
                    r.PersistenceId == persistenceId &&
                    r.SequenceNumber > minSequenceNumber)
                .Select(r => (long?)r.SequenceNumber);

        private static IQueryable<long?> MaxSeqForPersistenceIdQueryableCompatibilityModeWithMinId(
            DataConnection connection,
            string persistenceId,
            long minSequenceNumber)
            => connection
                .GetTable<JournalRow>()
                .Where(r =>
                    r.PersistenceId == persistenceId &&
                    r.SequenceNumber > minSequenceNumber)
                .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue())
                .Union(
                    connection
                        .GetTable<JournalMetaData>()
                        .Where(r =>
                            r.SequenceNumber > minSequenceNumber &&
                            r.PersistenceId == persistenceId)
                        .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue()));

        private static IQueryable<long?> MaxSeqForPersistenceIdQueryableCompatibilityMode(
            DataConnection connection,
            string persistenceId)
            => connection
                .GetTable<JournalRow>()
                .Where(r => r.PersistenceId == persistenceId)
                .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue())
                .Union(
                    connection
                        .GetTable<JournalMetaData>()
                        .Where(r => r.PersistenceId == persistenceId)
                        .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue()));

        private static Func<Util.Try<(IPersistentRepresentation, IImmutableSet<string>, long)>, Util.Try<ReplayCompletion>> MessageWithBatchMapper()
            => sertry => sertry.IsSuccess
                ? new Util.Try<ReplayCompletion>(new ReplayCompletion(sertry.Success.Value))
                : new Util.Try<ReplayCompletion>(sertry.Failure.Value);
    }

    public sealed class PersistenceIdAndSequenceNumber
    {
        public PersistenceIdAndSequenceNumber(long sequenceNumber, string persistenceId)
        {
            SequenceNumber = sequenceNumber;
            PersistenceId = persistenceId;
        }

        public long SequenceNumber { get; }
        public string PersistenceId { get; }
    }
}