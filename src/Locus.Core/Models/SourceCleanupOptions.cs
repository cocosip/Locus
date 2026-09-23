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
    }
}
