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
        /// Validates option values.
        /// </summary>
        public void Validate()
        {
            if (CompletionGuardStripeCount <= 0)
                throw new InvalidOperationException("StoragePool.CompletionGuardStripeCount must be greater than zero");
        }
    }
}
