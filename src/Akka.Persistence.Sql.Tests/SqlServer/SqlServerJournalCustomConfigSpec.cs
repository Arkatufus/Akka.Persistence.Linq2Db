﻿// -----------------------------------------------------------------------
//  <copyright file="SqlServerJournalCustomConfigSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Persistence.Sql.Tests.Common.Containers;
using Akka.Persistence.TCK.Journal;
using FluentAssertions.Extensions;
using LinqToDB;
using Xunit;
using Xunit.Abstractions;
#if !DEBUG
using Akka.Persistence.Sql.Tests.Common.Internal.Xunit;
#endif

namespace Akka.Persistence.Sql.Tests.SqlServer
{
#if !DEBUG
    [SkipWindows]
#endif
    [Collection(nameof(SqlServerPersistenceSpec))]
    public class SqlServerJournalCustomConfigSpec : JournalSpec
    {
        public SqlServerJournalCustomConfigSpec(ITestOutputHelper output, SqlServerContainer fixture)
            : base(Configuration(fixture), nameof(SqlServerJournalCustomConfigSpec), output)
        {
            if (!fixture.InitializeDbAsync().Wait(10.Seconds()))
                throw new Exception("Failed to clean up database in 10 seconds");
            Initialize();
        }

        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
        protected override bool SupportsSerialization => false;

        private static Configuration.Config Configuration(SqlServerContainer fixture)
            => SqlJournalDefaultSpecConfig.GetCustomConfig(
                "customSpec",
                "customJournalTable",
                "customMetadataTable",
                ProviderName.SqlServer2017,
                fixture.ConnectionString,
                true);
    }
}
