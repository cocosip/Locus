using System;
using Locus.Storage;

namespace Locus
{
    /// <summary>
    /// Tunable options for StoragePool concurrency behavior.
    /// Supports appsettings binding under Locus:StoragePool.
    /// </summary>
    public class StoragePoolOptions
    {
        /// <summary>
        /// Gets or sets the number of stripes used by completion guards.
        /// Higher values reduce lock collisions when many distinct file keys complete concurrently.
        /// Default: 256.
        /// </summary>
        public int CompletionGuardStripeCount { get; set; } = StoragePool.DefaultCompletionGuardStripeCount;

        /// <summary>
        /// Gets or sets the maximum number of timed-out Processing files to reclaim synchronously
        /// after an empty pending-file lease attempt.
        /// Default: 32.
        /// </summary>
        public int EmptyQueueTimedOutReclaimBatchSize { get; set; } = FileScheduler.DefaultEmptyQueueTimedOutReclaimBatchSize;

        /// <summary>
        /// Gets or sets the maximum number of timed-out Processing files to reclaim in a low-frequency
        /// background pass while normal leasing still succeeds.
        /// Default: 8.
        /// </summary>
        public int BackgroundTimedOutReclaimBatchSize { get; set; } = FileScheduler.DefaultBackgroundTimedOutReclaimBatchSize;

        /// <summary>
        /// Gets or sets the minimum delay between background timed-out reclaim attempts for the same tenant.
        /// Default: 30 seconds.
        /// </summary>
        public TimeSpan TimedOutReclaimCooldown { get; set; } = FileScheduler.DefaultTimedOutReclaimCooldown;

        /// <summary>
        /// Gets or sets whether low-frequency background timed-out reclaim is enabled.
        /// Default: true.
        /// </summary>
        public bool EnableBackgroundTimedOutReclaim { get; set; } = true;

        /// <summary>
        /// Validates option values.
        /// </summary>
        public void Validate()
        {
            if (CompletionGuardStripeCount <= 0)
                throw new InvalidOperationException("StoragePool.CompletionGuardStripeCount must be greater than zero");
            if (EmptyQueueTimedOutReclaimBatchSize < 0)
                throw new InvalidOperationException("StoragePool.EmptyQueueTimedOutReclaimBatchSize must be zero or greater");
            if (BackgroundTimedOutReclaimBatchSize < 0)
                throw new InvalidOperationException("StoragePool.BackgroundTimedOutReclaimBatchSize must be zero or greater");
            if (TimedOutReclaimCooldown < TimeSpan.Zero)
                throw new InvalidOperationException("StoragePool.TimedOutReclaimCooldown must be zero or greater");
        }
    }
}
