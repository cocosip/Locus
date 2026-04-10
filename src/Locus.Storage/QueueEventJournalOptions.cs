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
        /// Gets or sets the debounce interval used before persisting queue.state.json
        /// after accepted journal appends. Set to <see cref="TimeSpan.Zero"/> to force
        /// immediate state-file persistence on every append (legacy behavior).
        /// Default: 1 second.
        /// </summary>
        public TimeSpan StateFlushDebounce { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Gets or sets the linger window used by the per-tenant writer to coalesce
        /// queued append requests into a micro-batch when backlog exists.
        /// Default: 1 millisecond.
        /// </summary>
        public TimeSpan Linger { get; set; } = TimeSpan.FromMilliseconds(1);

        /// <summary>
        /// Gets or sets the maximum number of queued append requests that may be
        /// coalesced into a single journal write.
        /// Default: 16.
        /// </summary>
        public int MaxBatchRecords { get; set; } = 16;

        /// <summary>
        /// Gets or sets the maximum serialized payload size in bytes that may be
        /// coalesced into a single journal write.
        /// Default: 262144 bytes.
        /// </summary>
        public int MaxBatchBytes { get; set; } = 256 * 1024;

        /// <summary>
        /// Gets or sets the idle timeout after which the per-tenant append stream is closed.
        /// Default: 30 seconds.
        /// </summary>
        public TimeSpan WriterIdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the append acknowledgment mode.
        /// Default: <see cref="QueueEventJournalAckMode.Durable"/>.
        /// </summary>
        public QueueEventJournalAckMode AckMode { get; set; } = QueueEventJournalAckMode.Durable;

        /// <summary>
        /// Gets or sets the flush window used by <see cref="QueueEventJournalAckMode.Balanced"/>.
        /// Default: 5 milliseconds.
        /// </summary>
        public TimeSpan BalancedFlushWindow { get; set; } = TimeSpan.FromMilliseconds(5);

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
        /// Enabled by default so processed queue logs do not grow without bound once
        /// snapshot-based recovery is available in normal runtime operation.
        /// Default: true.
        /// </summary>
        public bool EnableCompaction { get; set; } = true;

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

            if (StateFlushDebounce < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.StateFlushDebounce cannot be negative");

            if (Linger < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.Linger cannot be negative");

            if (MaxBatchRecords <= 0)
                throw new InvalidOperationException("QueueEventJournal.MaxBatchRecords must be greater than zero");

            if (MaxBatchBytes <= 0)
                throw new InvalidOperationException("QueueEventJournal.MaxBatchBytes must be greater than zero");

            if (WriterIdleTimeout < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.WriterIdleTimeout cannot be negative");

            if (BalancedFlushWindow < TimeSpan.Zero)
                throw new InvalidOperationException("QueueEventJournal.BalancedFlushWindow cannot be negative");

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
