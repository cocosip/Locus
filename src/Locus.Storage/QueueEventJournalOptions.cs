using System;

namespace Locus.Storage
{
    /// <summary>
    /// Configures the durable per-tenant queue journal and projection worker.
    /// </summary>
    public class QueueEventJournalOptions
    {
        /// <summary>
        /// Gets or sets the root directory containing tenant journal subdirectories.
        /// Default: "./locus-queue".
        /// </summary>
        public string QueueDirectory { get; set; } = "./locus-queue";

        /// <summary>
        /// Gets or sets a value indicating whether the queue journal should be enabled.
        /// Default: true.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets a value indicating whether the legacy non-journal execution path is allowed.
        /// This compatibility switch should remain false for normal production deployments.
        /// Default: false.
        /// </summary>
        public bool AllowLegacyNonJournalMode { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the background projection worker is enabled.
        /// Default: true.
        /// </summary>
        public bool EnableProjection { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum number of journal records to project for a tenant in one cycle.
        /// Default: 64.
        /// </summary>
        public int MaxRecordsPerTenantPerCycle { get; set; } = 64;

        /// <summary>
        /// Gets or sets the maximum number of tenant journals to process in one cycle.
        /// Default: 8.
        /// </summary>
        public int MaxTenantsPerCycle { get; set; } = 8;

        /// <summary>
        /// Gets or sets the delay between projection cycles when work remains.
        /// Default: 500 milliseconds.
        /// </summary>
        public TimeSpan BusyCycleDelay { get; set; } = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Gets or sets the delay between projection cycles when no work is available.
        /// Default: 5 seconds.
        /// </summary>
        public TimeSpan IdleCycleDelay { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Gets or sets the maximum duration budget for a single projection cycle.
        /// Default: 2 seconds.
        /// </summary>
        public TimeSpan MaxProjectionTimePerCycle { get; set; } = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Gets or sets a value indicating whether projection snapshots should be refreshed automatically
        /// after a tenant catches up to the current journal tail.
        /// Default: true.
        /// </summary>
        public bool EnableAutomaticSnapshots { get; set; } = true;

        /// <summary>
        /// Gets or sets the minimum time between automatic snapshot refreshes for the same tenant
        /// when new journal progress exists.
        /// Default: 15 minutes.
        /// </summary>
        public TimeSpan AutomaticSnapshotInterval { get; set; } = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Gets or sets the minimum additional projected bytes before an automatic snapshot is refreshed.
        /// Default: 1 MB.
        /// </summary>
        public long MinBytesBeforeAutomaticSnapshot { get; set; } = 1L * 1024L * 1024L;

        /// <summary>
        /// Gets or sets a value indicating whether projected tenant journals should be compacted.
        /// This remains disabled by default so deployments can opt in only after validating
        /// snapshot-based rebuild behavior for their workload.
        /// Default: false.
        /// </summary>
        public bool EnableCompaction { get; set; } = false;

        /// <summary>
        /// Gets or sets the minimum processed bytes before a tenant journal is compacted.
        /// Default: 4 MB.
        /// </summary>
        public long MinBytesBeforeCompaction { get; set; } = 4L * 1024L * 1024L;

        /// <summary>
        /// Validates option values.
        /// </summary>
        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(QueueDirectory))
                throw new InvalidOperationException("QueueEventJournal.QueueDirectory cannot be empty");

            if (!Enabled && !AllowLegacyNonJournalMode)
            {
                throw new InvalidOperationException(
                    "QueueEventJournal.Enabled=false is blocked by default. " +
                    "Set QueueEventJournal.AllowLegacyNonJournalMode=true only for explicit compatibility or test scenarios.");
            }

            if (!Enabled && EnableProjection)
            {
                throw new InvalidOperationException(
                    "QueueEventJournal.EnableProjection cannot be true when QueueEventJournal.Enabled is false.");
            }

            if (MaxRecordsPerTenantPerCycle <= 0)
                throw new InvalidOperationException("QueueEventJournal.MaxRecordsPerTenantPerCycle must be greater than zero");

            if (MaxTenantsPerCycle <= 0)
                throw new InvalidOperationException("QueueEventJournal.MaxTenantsPerCycle must be greater than zero");

            if (BusyCycleDelay < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.BusyCycleDelay cannot be negative");

            if (IdleCycleDelay < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.IdleCycleDelay cannot be negative");

            if (MaxProjectionTimePerCycle <= TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.MaxProjectionTimePerCycle must be greater than zero");

            if (AutomaticSnapshotInterval < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.AutomaticSnapshotInterval cannot be negative");

            if (MinBytesBeforeAutomaticSnapshot < 0)
                throw new InvalidOperationException("QueueEventJournal.MinBytesBeforeAutomaticSnapshot cannot be negative");

            if (MinBytesBeforeCompaction < 0)
                throw new InvalidOperationException("QueueEventJournal.MinBytesBeforeCompaction cannot be negative");
        }
    }
}
