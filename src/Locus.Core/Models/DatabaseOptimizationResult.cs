namespace Locus.Core.Models
{
    /// <summary>
    /// Result of database optimization operation.
    /// </summary>
    public class DatabaseOptimizationResult
    {
        /// <summary>
        /// Gets or sets the number of metadata databases optimized.
        /// </summary>
        public int MetadataDatabasesOptimized { get; set; }

        /// <summary>
        /// Gets or sets the number of quota databases optimized.
        /// </summary>
        public int QuotaDatabasesOptimized { get; set; }

        /// <summary>
        /// Gets or sets the total space reclaimed (in bytes).
        /// </summary>
        public long SpaceReclaimed { get; set; }

        /// <summary>
        /// Gets or sets the total size before optimization (in bytes).
        /// </summary>
        public long SizeBefore { get; set; }

        /// <summary>
        /// Gets or sets the total size after optimization (in bytes).
        /// </summary>
        public long SizeAfter { get; set; }

        /// <summary>
        /// Gets the space reclaimed in megabytes.
        /// </summary>
        public double SpaceReclaimedMB => SpaceReclaimed / 1024.0 / 1024.0;

        /// <summary>
        /// Gets the percentage of space reclaimed.
        /// </summary>
        public double PercentageReclaimed => SizeBefore > 0 ? (SpaceReclaimed * 100.0 / SizeBefore) : 0;
    }
}
