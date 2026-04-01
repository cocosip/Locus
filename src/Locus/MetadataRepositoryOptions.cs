using System;

namespace Locus
{
    /// <summary>
    /// Tunable options for MetadataRepository write-behind behavior.
    /// Supports appsettings binding under Locus:MetadataRepository.
    /// </summary>
    public class MetadataRepositoryOptions
    {
        /// <summary>
        /// Default max number of operations buffered in the persistence channel.
        /// </summary>
        public const int DefaultMaxQueueSize = 100_000;

        /// <summary>
        /// Default max operations drained per persistence loop batch.
        /// </summary>
        public const int DefaultDrainBatchSize = 2_000;

        /// <summary>
        /// Default soft threshold (percentage of queue capacity) before coalescing starts.
        /// </summary>
        public const int DefaultSoftMergeThresholdPercent = 90;

        /// <summary>
        /// Default number of records loaded per startup batch when rebuilding in-memory active indexes.
        /// </summary>
        public const int DefaultStartupLoadBatchSize = 2_000;

        /// <summary>
        /// Default timeout in seconds for draining the persistence queue during shutdown.
        /// </summary>
        public const int DefaultShutdownDrainTimeoutSeconds = 30;

        /// <summary>
        /// Default interval in seconds between persistence loop drain cycles when no new channel items arrive.
        /// </summary>
        public const int DefaultPersistenceIntervalSeconds = 2;

        /// <summary>
        /// Gets or sets whether background write-behind persistence is enabled.
        /// Default: true.
        /// </summary>
        public bool EnableBackgroundPersistence { get; set; } = true;

        /// <summary>
        /// Gets or sets max buffered operations in the persistence channel.
        /// Default: 100000.
        /// </summary>
        public int MaxQueueSize { get; set; } = DefaultMaxQueueSize;

        /// <summary>
        /// Gets or sets max operations drained per loop batch.
        /// Default: 2000.
        /// </summary>
        public int DrainBatchSize { get; set; } = DefaultDrainBatchSize;

        /// <summary>
        /// Gets or sets the soft merge threshold percentage (1-100).
        /// Default: 90.
        /// </summary>
        public int SoftMergeThresholdPercent { get; set; } = DefaultSoftMergeThresholdPercent;

        /// <summary>
        /// Gets or sets the startup active-file load batch size.
        /// Default: 2000.
        /// </summary>
        public int StartupLoadBatchSize { get; set; } = DefaultStartupLoadBatchSize;

        /// <summary>
        /// Gets or sets the timeout in seconds for draining the persistence queue during graceful shutdown.
        /// A larger value reduces the chance of losing in-flight writes (and thus creating orphan files).
        /// Default: 30.
        /// </summary>
        public int ShutdownDrainTimeoutSeconds { get; set; } = DefaultShutdownDrainTimeoutSeconds;

        /// <summary>
        /// Gets or sets the interval in seconds between persistence loop drain cycles
        /// when no new channel items arrive. Controls how quickly coalesced-only writes are flushed.
        /// Default: 5.
        /// </summary>
        public int PersistenceIntervalSeconds { get; set; } = DefaultPersistenceIntervalSeconds;

        /// <summary>
        /// Validates option values.
        /// </summary>
        public void Validate()
        {
            if (MaxQueueSize <= 0)
                throw new InvalidOperationException("MetadataRepository.MaxQueueSize must be greater than zero");

            if (DrainBatchSize <= 0)
                throw new InvalidOperationException("MetadataRepository.DrainBatchSize must be greater than zero");

            if (SoftMergeThresholdPercent <= 0 || SoftMergeThresholdPercent > 100)
                throw new InvalidOperationException("MetadataRepository.SoftMergeThresholdPercent must be between 1 and 100");

            if (StartupLoadBatchSize <= 0)
                throw new InvalidOperationException("MetadataRepository.StartupLoadBatchSize must be greater than zero");

            if (ShutdownDrainTimeoutSeconds <= 0)
                throw new InvalidOperationException("MetadataRepository.ShutdownDrainTimeoutSeconds must be greater than zero");

            if (PersistenceIntervalSeconds <= 0)
                throw new InvalidOperationException("MetadataRepository.PersistenceIntervalSeconds must be greater than zero");
        }
    }
}
