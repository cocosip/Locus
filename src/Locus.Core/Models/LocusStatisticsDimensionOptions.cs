namespace Locus.Core.Models
{
    /// <summary>
    /// Controls which low-cardinality dimensions are retained by in-process Locus statistics.
    /// </summary>
    public class LocusStatisticsDimensionOptions
    {
        /// <summary>
        /// Gets or sets whether tenant_id is retained. Disabled by default to avoid high cardinality.
        /// </summary>
        public bool TenantId { get; set; }

        /// <summary>
        /// Gets or sets whether volume_id is retained.
        /// </summary>
        public bool VolumeId { get; set; } = true;

        /// <summary>
        /// Gets or sets whether watcher_id is retained.
        /// </summary>
        public bool WatcherId { get; set; } = true;

        /// <summary>
        /// Gets or sets whether operation is retained.
        /// </summary>
        public bool Operation { get; set; } = true;
    }
}
