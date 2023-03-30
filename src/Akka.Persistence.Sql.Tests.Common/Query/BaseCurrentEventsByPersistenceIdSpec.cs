﻿// -----------------------------------------------------------------------
//  <copyright file="BaseCurrentEventsByPersistenceIdSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Akka.Configuration;
using Akka.Persistence.Query;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Query;
using Akka.Persistence.Sql.Tests.Common.Containers;
using Akka.Persistence.TCK.Query;
using FluentAssertions.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Sql.Tests.Common.Query
{
    public abstract class BaseCurrentEventsByPersistenceIdSpec<T> : CurrentEventsByPersistenceIdSpec where T : ITestContainer
    {
        protected BaseCurrentEventsByPersistenceIdSpec(TagMode tagMode, ITestOutputHelper output, string name, T fixture)
            : base(Config(tagMode, fixture), name, output)
        {
            ReadJournal = Sys.ReadJournalFor<SqlReadJournal>(SqlReadJournal.Identifier);
        }

        // Unit tests suffer from racy condition where the read journal tries to access the database
        // when the write journal has not been initialized
        [Fact]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_empty_journal()
        {
            Persistence.Instance.Get(Sys).JournalFor(null);
            Thread.Sleep(500);
            base.ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_empty_journal();
        }

        // Unit tests suffer from racy condition where the read journal tries to access the database
        // when the write journal has not been initialized
        [Fact]
        public override void ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_empty_journal_from_0_to_0()
        {
            Persistence.Instance.Get(Sys).JournalFor(null);
            Thread.Sleep(500);
            base.ReadJournal_CurrentEventsByPersistenceId_should_return_empty_stream_for_empty_journal_from_0_to_0();
        }

        private static Configuration.Config Config(TagMode tagMode, T fixture)
        {
            if (!fixture.InitializeDbAsync().Wait(10.Seconds()))
                throw new Exception("Failed to clean up database in 10 seconds");
            
            return ConfigurationFactory.ParseString($@"
akka.loglevel = INFO
akka.persistence {{
    journal {{
        plugin = ""akka.persistence.journal.sql""
        auto-start-journals = [ ""akka.persistence.journal.sql"" ]
        sql {{
            provider-name = ""{fixture.ProviderName}""
            tag-write-mode = ""{tagMode}""
            connection-string = ""{fixture.ConnectionString}""
            auto-initialize = on
        }}
    }}
    query {{
        journal {{
            sql {{
                provider-name = ""{fixture.ProviderName}""
                connection-string = ""{fixture.ConnectionString}""
                tag-read-mode = ""{tagMode}""
                auto-initialize = on
                refresh-interval = 1s
            }}
        }}
    }}
}}
akka.test.single-expect-default = 10s")
                .WithFallback(SqlPersistence.DefaultConfiguration);
        }
    }
}
