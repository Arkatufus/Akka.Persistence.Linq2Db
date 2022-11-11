﻿// -----------------------------------------------------------------------
//  <copyright file="DataCompatibilitySpecBase.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2022 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Hosting;
using Akka.Persistence.Linq2Db.Data.Compatibility.Tests.Internal;
using Akka.Persistence.Query;
using Akka.Persistence.Sql.Compat.Common;
using Akka.Persistence.Sql.Linq2Db;
using Akka.Persistence.Sql.Linq2Db.Query;
using Akka.Streams;
using FluentAssertions;
using LanguageExt.UnitsOfMeasure;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Akka.Persistence.Linq2Db.Data.Compatibility.Tests
{
    public abstract class TestSettings
    {
        public abstract string JournalTableName { get; }
        
        public abstract string JournalMetadataTableName { get; }
        
        public abstract string SnapshotTableName { get; }
        
        public abstract string ProviderName { get; }
        
        public abstract string CompatibilityMode { get; }
        
        public virtual string? SchemaName { get; } = null;
    }
    
    public abstract class DataCompatibilitySpecBase<T>: IAsyncLifetime where T: ITestContainer, new()
    {
        protected TestCluster? TestCluster { get; private set; }
        protected ITestContainer Fixture { get; }
        protected ITestOutputHelper Output { get; }

        protected abstract TestSettings Settings { get; }
        
        protected DataCompatibilitySpecBase(ITestOutputHelper output)
        {
            Output = output;
            Fixture = new T();
        }

        private Config Config()
        {
            return (Config) $@"
akka.persistence {{
	journal {{
		plugin = ""akka.persistence.journal.linq2db""
		linq2db {{
			connection-string = ""{Fixture.ConnectionString}""
			provider-name = {Settings.ProviderName}
			table-compatibility-mode = {Settings.CompatibilityMode}
			tables.journal {{ 
				auto-init = false
				schema-name = {Settings.SchemaName ?? "null"}
				table-name = {Settings.JournalTableName} 
				metadata-table-name = {Settings.JournalMetadataTableName}
			}}
		}}
	}}

	query.journal.linq2db {{
		connection-string = ""{Fixture.ConnectionString}""
		provider-name = {Settings.ProviderName}
		table-compatibility-mode = {Settings.CompatibilityMode}
		tables.journal {{ 
			auto-init = false
			table-name = {Settings.JournalTableName}
			metadata-table-name = {Settings.JournalMetadataTableName}
		}}
	}}

	snapshot-store {{
		plugin = akka.persistence.snapshot-store.linq2db
		linq2db {{
			connection-string = ""{Fixture.ConnectionString}""
			provider-name = {Settings.ProviderName}
			table-compatibility-mode = {Settings.CompatibilityMode}
			tables {{
				snapshot {{ 
					auto-init = false
					schema-name = {Settings.SchemaName ?? "null"}
					table-name = {Settings.SnapshotTableName}
				}}
			}}
		}}
	}}
}}";
        }

        protected virtual void CustomSetup(AkkaConfigurationBuilder builder, IServiceProvider provider)
        {
        }

        protected abstract void Setup(AkkaConfigurationBuilder builder, IServiceProvider provider);
        
        private void InternalSetup(AkkaConfigurationBuilder builder, IServiceProvider provider)
        {
            builder.AddHocon(
                Config().WithFallback(Linq2DbPersistence.DefaultConfiguration()), 
                HoconAddMode.Prepend);
            Setup(builder, provider);
        }

        public async Task InitializeAsync()
        {
            await Fixture.InitializeAsync();
            TestCluster = new TestCluster(InternalSetup, "linq2db", Output);
            await TestCluster.StartAsync();
        }

        public async Task DisposeAsync()
        {
            if(TestCluster is { })
                await TestCluster.DisposeAsync();
            await Fixture.DisposeAsync();
        }

        protected async Task TruncateEventsToLastSnapshot()
        {
            var region = TestCluster!.ShardRegions[0];
            var cts = new CancellationTokenSource(10.Seconds());
            try
            {
                var tasks = Enumerable.Range(0, 100).Select(id =>
                    region.Ask<(string, StateSnapshot?)>(new Truncate(id), cts.Token)).ToList();
                while (tasks.Count > 0)
                {
                    var task = await Task.WhenAny(tasks);
                    if (cts.Token.IsCancellationRequested)
                        break;

                    tasks.Remove(task);
                    var (id, lastSnapshot) = task.Result;
                    if (lastSnapshot is { })
                        Output.WriteLine(
                            $"{id} data truncated. " +
                            $"Snapshot: [Total: {lastSnapshot.Total}, Persisted: {lastSnapshot.Persisted}]");
                    else
                        throw new XunitException($"Failed to truncate events for entity {id}");
                }

                if (cts.IsCancellationRequested)
                {
                    throw new TimeoutException("Failed to truncate all data within 10 seconds");
                }
            }
            finally
            {
                cts.Dispose();
            }
        }
        
        protected static void ValidateState(string persistentId, StateSnapshot lastSnapshot, int count, int persisted)
        {
            persisted.Should().Be(36, "Entity {0} should have persisted 36 events", persistentId);
            lastSnapshot.Persisted.Should().Be(24, "Entity {0} last snapshot should have persisted 24 events", persistentId);

            var baseValue = int.Parse(persistentId) * 3;
            var roundTotal = (baseValue * 3 + 3) * 4;
            lastSnapshot.Total.Should().Be(roundTotal * 2, "Entity {0} last snapshot total should be {1}", persistentId, roundTotal * 2);
            count.Should().Be(roundTotal * 3, "Entity {0} total should be {1}", persistentId, roundTotal * 3);
        }

        protected static async Task ValidateTags(ActorSystem system)
        {
            var readJournal = PersistenceQuery.Get(system)
                .ReadJournalFor<Linq2DbReadJournal>(Linq2DbReadJournal.Identifier);

            // "Tag2" check
            var roundTotal = Enumerable.Range(0, 300)
                .Where(i => i % 3 == 2)
                .Aggregate(0, (accum, val) => accum + val);
            
            var events = await readJournal.CurrentEventsByTag("Tag2", Offset.NoOffset())
                .RunAsAsyncEnumerable(system.Materializer()).ToListAsync();
            events.Count.Should().Be(800);

            var intMessages = events.Where(e => e.Event is int).Select(e => (int)e.Event).ToList();
            intMessages.Count.Should().Be(200);
            intMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            var strMessages = events.Where(e => e.Event is string).Select(e => int.Parse((string)e.Event)).ToList();
            strMessages.Count.Should().Be(200);
            strMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            var shardMessages = events.Where(e => e.Event is ShardedMessage)
                .Select(e => ((ShardedMessage)e.Event).Message).ToList();
            shardMessages.Count.Should().Be(200);
            shardMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            var customShardMessages = events.Where(e => e.Event is CustomShardedMessage)
                .Select(e => ((CustomShardedMessage)e.Event).Message).ToList();
            customShardMessages.Count.Should().Be(200);
            customShardMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            // "Tag1" check, there should be twice as much "Tag1" as "Tag2"
            roundTotal = Enumerable.Range(0, 300)
                .Where(i => i % 3 == 2 || i % 3 == 1)
                .Aggregate(0, (accum, val) => accum + val);
            
            events = await readJournal.CurrentEventsByTag("Tag1", Offset.NoOffset())
                .RunAsAsyncEnumerable(system.Materializer()).ToListAsync();
            events.Count.Should().Be(1600);
            
            intMessages = events.Where(e => e.Event is int).Select(e => (int)e.Event).ToList();
            intMessages.Count.Should().Be(400);
            intMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            strMessages = events.Where(e => e.Event is string).Select(e => int.Parse((string)e.Event)).ToList();
            strMessages.Count.Should().Be(400);
            strMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            shardMessages = events.Where(e => e.Event is ShardedMessage)
                .Select(e => ((ShardedMessage)e.Event).Message).ToList();
            shardMessages.Count.Should().Be(400);
            shardMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
            
            customShardMessages = events.Where(e => e.Event is CustomShardedMessage)
                .Select(e => ((CustomShardedMessage)e.Event).Message).ToList();
            customShardMessages.Count.Should().Be(400);
            customShardMessages.Aggregate(0, (accum, val) => accum + val).Should().Be(roundTotal * 2);
        }

        protected static async Task ValidateRecovery(IActorRef region)
        {
            foreach (var id in Enumerable.Range(0, 100))
            {
                var (persistenceId, lastSnapshot, total, persisted) =
                    await region.Ask<(string, StateSnapshot, int, int)>(new Start(id));
                ValidateState(persistenceId, lastSnapshot, total, persisted);
            }
        }
    }
}