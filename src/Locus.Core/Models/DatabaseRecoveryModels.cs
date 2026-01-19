using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Result of a database rebuild operation.
    /// </summary>
    public class DatabaseRebuildResult
    {
        /// <summary>
        /// Gets or sets the type of database (Metadata, Quota).
        /// </summary>
        public string DatabaseType { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant ID.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the path to the database file.
        /// </summary>
        public string DatabasePath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the path to the backup of the corrupted database.
        /// </summary>
        public string? BackupPath { get; set; }

        /// <summary>
        /// Gets or sets whether the rebuild was successful.
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Gets or sets the number of records rebuilt.
        /// </summary>
        public int RecordsRebuilt { get; set; }

        /// <summary>
        /// Gets the list of errors that occurred during rebuild.
        /// </summary>
        public List<string> Errors { get; set; } = new List<string>();
    }

    /// <summary>
    /// Health report for all databases.
    /// </summary>
    public class DatabaseHealthReport
    {
        /// <summary>
        /// Gets or sets the number of healthy databases.
        /// </summary>
        public int HealthyDatabases { get; set; }

        /// <summary>
        /// Gets the list of corrupted databases.
        /// </summary>
        public List<DatabaseHealthInfo> CorruptedDatabases { get; set; } = new List<DatabaseHealthInfo>();

        /// <summary>
        /// Gets whether all databases are healthy.
        /// </summary>
        public bool AllHealthy => CorruptedDatabases.Count == 0;
    }

    /// <summary>
    /// Health information for a single database.
    /// </summary>
    public class DatabaseHealthInfo
    {
        /// <summary>
        /// Gets or sets the type of database (Metadata, Quota).
        /// </summary>
        public string DatabaseType { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant ID.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the path to the database file.
        /// </summary>
        public string DatabasePath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether the database is corrupted.
        /// </summary>
        public bool IsCorrupted { get; set; }
    }
}
