using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Aggregated statistics for a queried time range.
    /// </summary>
    public sealed class LocusStatisticsSnapshot
    {
        /// <summary>
        /// Gets or sets the number of successful file writes.
        /// </summary>
        public long WriteFileCount { get; set; }

        /// <summary>
        /// Gets or sets written bytes.
        /// </summary>
        public long WriteBytes { get; set; }

        /// <summary>
        /// Gets or sets write throughput in MiB per second across the queried time range.
        /// </summary>
        public double WriteMegabytesPerSecond { get; set; }

        /// <summary>
        /// Gets or sets the number of files dequeued for processing.
        /// </summary>
        public long DequeuedFileCount { get; set; }

        /// <summary>
        /// Gets or sets the number of files read directly.
        /// </summary>
        public long ReadFileCount { get; set; }

        /// <summary>
        /// Gets or sets the number of files marked completed.
        /// </summary>
        public long CompletedFileCount { get; set; }

        /// <summary>
        /// Gets or sets the number of metadata SQLite operations persisted.
        /// </summary>
        public long SqlitePersistedOperationCount { get; set; }

        /// <summary>
        /// Gets or sets the number of files imported by watchers.
        /// </summary>
        public long WatcherImportedFileCount { get; set; }

        /// <summary>
        /// Gets or sets bytes imported by watchers.
        /// </summary>
        public long WatcherImportedBytes { get; set; }

        /// <summary>
        /// Gets aggregated measurements.
        /// </summary>
        public List<LocusStatisticsMeasurement> Measurements { get; } = new List<LocusStatisticsMeasurement>();
    }
}
