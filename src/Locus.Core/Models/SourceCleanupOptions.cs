using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Controls the independent source-file cleanup worker used by file watchers.
    /// </summary>
    public sealed class SourceCleanupOptions
    {
        /// <summary>Gets or sets whether the source cleanup worker is enabled.</summary>
        public bool Enabled { get; set; } = true;

        /// <summary>Gets or sets the SQLite database path containing active cleanup jobs.</summary>
        public string DatabasePath { get; set; } = "./locus-watchers/source-cleanup.db";

        /// <summary>Gets or sets the interval between cleanup worker scans.</summary>
        public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>Gets or sets the maximum number of source actions processed concurrently.</summary>
        public int MaxConcurrentActions { get; set; } = 2;

        /// <summary>Gets or sets the maximum number of active source cleanup records.</summary>
        /// <remarks>New watcher imports are deferred while this limit is reached.</remarks>
        public int MaxActiveJobs { get; set; } = 10000;

        /// <summary>Gets or sets how long terminal Keep/Failed records are retained.</summary>
        public TimeSpan TerminalJobRetentionPeriod { get; set; } = TimeSpan.FromDays(1);

        /// <summary>Gets or sets the age after which an interrupted import reservation is recoverable.</summary>
        public TimeSpan ImportReservationTimeout { get; set; } = TimeSpan.FromMinutes(10);

        /// <summary>Gets or sets whether source cleanup SQLite maintenance is enabled.</summary>
        public bool EnableDatabaseOptimization { get; set; } = true;

        /// <summary>Gets or sets the minimum interval between source cleanup database optimizations.</summary>
        public TimeSpan DatabaseOptimizationInterval { get; set; } = TimeSpan.FromDays(1);

        /// <summary>Gets or sets the maximum number of terminal rows removed per maintenance pass.</summary>
        public int TerminalPruneBatchSize { get; set; } = 5000;

        /// <summary>Validates source cleanup option values.</summary>
        public void Validate()
        {
            if (Enabled && string.IsNullOrWhiteSpace(DatabasePath))
                throw new InvalidOperationException("SourceCleanup.DatabasePath is required when source cleanup is enabled");
            if (PollingInterval <= TimeSpan.Zero)
                throw new InvalidOperationException("SourceCleanup.PollingInterval must be greater than zero");
            if (MaxConcurrentActions <= 0)
                throw new InvalidOperationException("SourceCleanup.MaxConcurrentActions must be greater than zero");
            if (MaxActiveJobs <= 0)
                throw new InvalidOperationException("SourceCleanup.MaxActiveJobs must be greater than zero");
            if (TerminalJobRetentionPeriod <= TimeSpan.Zero)
                throw new InvalidOperationException("SourceCleanup.TerminalJobRetentionPeriod must be greater than zero");
            if (ImportReservationTimeout <= TimeSpan.Zero)
                throw new InvalidOperationException("SourceCleanup.ImportReservationTimeout must be greater than zero");
            if (DatabaseOptimizationInterval <= TimeSpan.Zero)
                throw new InvalidOperationException("SourceCleanup.DatabaseOptimizationInterval must be greater than zero");
            if (TerminalPruneBatchSize <= 0)
                throw new InvalidOperationException("SourceCleanup.TerminalPruneBatchSize must be greater than zero");
        }
    }
}
