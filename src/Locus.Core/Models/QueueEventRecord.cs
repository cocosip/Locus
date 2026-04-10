using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents a durable queue event written to the tenant journal.
    /// </summary>
    public class QueueEventRecord
    {
        /// <summary>
        /// Gets or sets the schema version for future compatibility.
        /// </summary>
        public int SchemaVersion { get; set; } = 1;

        /// <summary>
        /// Gets or sets the event identifier.
        /// </summary>
        public string EventId { get; set; } = Guid.NewGuid().ToString("N");

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the file key.
        /// </summary>
        public string FileKey { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the event type.
        /// </summary>
        public QueueEventType EventType { get; set; }

        /// <summary>
        /// Gets or sets the event timestamp in UTC.
        /// </summary>
        public DateTime OccurredAtUtc { get; set; } = DateTime.UtcNow;

        /// <summary>
        /// Gets or sets the per-tenant monotonic sequence number assigned by the journal writer.
        /// </summary>
        public long? SequenceNumber { get; set; }

        /// <summary>
        /// Gets or sets the CRC32 checksum for the serialized record payload.
        /// </summary>
        public uint? PayloadCrc32 { get; set; }

        /// <summary>
        /// Gets or sets the volume identifier.
        /// </summary>
        public string? VolumeId { get; set; }

        /// <summary>
        /// Gets or sets the physical path.
        /// </summary>
        public string? PhysicalPath { get; set; }

        /// <summary>
        /// Gets or sets the logical directory path currently tracked by the pipeline.
        /// </summary>
        public string? DirectoryPath { get; set; }

        /// <summary>
        /// Gets or sets the file size in bytes.
        /// </summary>
        public long? FileSize { get; set; }

        /// <summary>
        /// Gets or sets the processing status associated with the event.
        /// </summary>
        public FileProcessingStatus? Status { get; set; }

        /// <summary>
        /// Gets or sets the processing lease start time.
        /// </summary>
        public DateTime? ProcessingStartTimeUtc { get; set; }

        /// <summary>
        /// Gets or sets the retry count.
        /// </summary>
        public int? RetryCount { get; set; }

        /// <summary>
        /// Gets or sets the earliest UTC timestamp when the file may be processed again.
        /// </summary>
        public DateTime? AvailableForProcessingAtUtc { get; set; }

        /// <summary>
        /// Gets or sets the failure message when applicable.
        /// </summary>
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// Gets or sets the original file name.
        /// </summary>
        public string? OriginalFileName { get; set; }

        /// <summary>
        /// Gets or sets the file extension.
        /// </summary>
        public string? FileExtension { get; set; }
    }
}
