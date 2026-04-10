using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Describes the projection state for a single tenant journal.
    /// </summary>
    public sealed class QueueProjectionTenantState
    {
        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the current projected cursor offset.
        /// </summary>
        public long CursorOffset { get; set; }

        /// <summary>
        /// Gets or sets the current journal size in bytes.
        /// </summary>
        public long JournalSizeBytes { get; set; }

        /// <summary>
        /// Gets or sets the remaining lag in bytes.
        /// </summary>
        public long LagBytes { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether a projection snapshot exists for the tenant.
        /// </summary>
        public bool HasSnapshot { get; set; }

        /// <summary>
        /// Gets or sets the UTC timestamp when the latest snapshot was created.
        /// </summary>
        public DateTime? SnapshotCreatedAtUtc { get; set; }

        /// <summary>
        /// Gets or sets the cursor offset captured by the latest snapshot.
        /// </summary>
        public long SnapshotCursorOffset { get; set; }

        /// <summary>
        /// Gets or sets the number of file records stored in the latest snapshot.
        /// </summary>
        public int SnapshotFileCount { get; set; }

        /// <summary>
        /// Gets or sets the last projected journal sequence number.
        /// </summary>
        public long LastProjectedSequenceNumber { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the projector has detected a sequence gap.
        /// </summary>
        public bool GapDetected { get; set; }

        /// <summary>
        /// Gets or sets the UTC timestamp when the latest sequence gap was detected.
        /// </summary>
        public DateTime? LastGapDetectedAtUtc { get; set; }

        /// <summary>
        /// Gets or sets the expected sequence number when the latest gap was detected.
        /// </summary>
        public long? GapExpectedSequenceNumber { get; set; }

        /// <summary>
        /// Gets or sets the observed sequence number when the latest gap was detected.
        /// </summary>
        public long? GapObservedSequenceNumber { get; set; }

        /// <summary>
        /// Gets or sets the active journal format detected for the tenant.
        /// </summary>
        public string? JournalFormat { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether a corrupt journal tail was detected.
        /// </summary>
        public bool JournalCorruptTailDetected { get; set; }

        /// <summary>
        /// Gets or sets the UTC timestamp when the latest corrupt journal tail was detected.
        /// </summary>
        public DateTime? LastJournalCorruptTailDetectedAtUtc { get; set; }

        /// <summary>
        /// Gets or sets the logical offset where the latest corrupt journal tail started.
        /// </summary>
        public long? LastJournalCorruptTailOffset { get; set; }

        /// <summary>
        /// Gets or sets the number of times the tenant journal has been auto-repaired.
        /// </summary>
        public int JournalAutoRepairCount { get; set; }
    }
}
