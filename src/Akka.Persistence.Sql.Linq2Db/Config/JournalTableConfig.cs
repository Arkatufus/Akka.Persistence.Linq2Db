﻿using System;
using Akka.Configuration;

namespace Akka.Persistence.Sql.Linq2Db.Config
{
    public class JournalTableConfig : IEquatable<JournalTableConfig>
    {
        public string SchemaName { get; }
        public EventJournalTableConfig EventJournalTable { get; }
        public MetadataTableConfig MetadataTable { get; }
        public JournalTableConfig(Configuration.Config config)
        {
            var mappingPath = config.GetString("table-mapping");
            if (string.IsNullOrEmpty(mappingPath))
                throw new ConfigurationException("The configuration property akka.persistence.journal.linq2db.table-mapping is null or empty");
            
            var mappingConfig = config.GetConfig(mappingPath);
            if (mappingConfig is null)
                throw new ConfigurationException($"The configuration path akka.persistence.journal.linq2db.{mappingPath} does not exist");

            if (mappingPath != "default")
                mappingConfig.WithFallback(config.GetString("default"));
            
            SchemaName = mappingConfig.GetString("schema-name");

            EventJournalTable = new EventJournalTableConfig(mappingConfig);
            MetadataTable = new MetadataTableConfig(mappingConfig);
        }

        public bool Equals(JournalTableConfig other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(EventJournalTable, other.EventJournalTable) &&
                   Equals(MetadataTable, other.MetadataTable) &&
                   SchemaName == other.SchemaName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj is JournalTableConfig j && Equals(j);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(EventJournalTable, SchemaName, MetadataTable);
        }
    }

}