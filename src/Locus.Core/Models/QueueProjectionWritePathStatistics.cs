namespace Locus.Core.Models
{
    /// <summary>
    /// Aggregated diagnostics for the observed projection enqueue write path.
    /// </summary>
    public sealed class QueueProjectionWritePathStatistics
    {
        /// <summary>
        /// Gets or sets the total number of observed projection writes.
        /// </summary>
        public long ProjectedWriteCount { get; set; }

        /// <summary>
        /// Gets or sets the number of validation phases observed.
        /// </summary>
        public long ValidationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent validating projected metadata.
        /// </summary>
        public long ValidationTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed cache/index update phases.
        /// </summary>
        public long CacheAndIndexCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent updating cache and indexes.
        /// </summary>
        public long CacheAndIndexTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed cache mutation phases.
        /// </summary>
        public long CacheMutationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent mutating the in-memory cache.
        /// </summary>
        public long CacheMutationTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed physical-path index phases.
        /// </summary>
        public long PhysicalPathIndexCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent updating the physical-path index.
        /// </summary>
        public long PhysicalPathIndexTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed pending-queue update phases.
        /// </summary>
        public long PendingQueueUpdateCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent updating pending-queue structures.
        /// </summary>
        public long PendingQueueTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed status-index update phases.
        /// </summary>
        public long StatusIndexUpdateCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent updating status indexes.
        /// </summary>
        public long StatusIndexTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed persistence enqueue phases.
        /// </summary>
        public long PersistenceEnqueueCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent enqueueing persistence work.
        /// </summary>
        public long PersistenceEnqueueTicks { get; set; }
    }
}
