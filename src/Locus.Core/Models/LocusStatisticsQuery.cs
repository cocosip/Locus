using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Query parameters for in-process statistics snapshots.
    /// </summary>
    public sealed class LocusStatisticsQuery
    {
        /// <summary>
        /// Gets or sets the inclusive lower bound.
        /// </summary>
        public DateTimeOffset From { get; set; }

        /// <summary>
        /// Gets or sets the exclusive upper bound.
        /// </summary>
        public DateTimeOffset To { get; set; }

        /// <summary>
        /// Gets or sets an optional tenant filter.
        /// </summary>
        public string? TenantId { get; set; }

        /// <summary>
        /// Gets or sets an optional volume filter.
        /// </summary>
        public string? VolumeId { get; set; }

        /// <summary>
        /// Gets or sets an optional watcher filter.
        /// </summary>
        public string? WatcherId { get; set; }

        /// <summary>
        /// Gets or sets an optional operation filter.
        /// </summary>
        public string? Operation { get; set; }
    }
}
