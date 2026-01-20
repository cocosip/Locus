using System;
using LiteDB;
using Locus.Core.Models;

namespace Locus.Storage.Data
{
    /// <summary>
    /// File metadata entity stored in the metadata repository.
    /// </summary>
    public class FileMetadata
    {
        /// <summary>
        /// Gets or sets the unique file key (identifier).
        /// </summary>
        [BsonId]
        public string FileKey { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant ID that owns this file.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the storage volume ID where the file is stored.
        /// </summary>
        public string VolumeId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the physical path to the file on the storage volume.
        /// </summary>
        public string PhysicalPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the directory path (for quota management).
        /// </summary>
        public string DirectoryPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the file size in bytes.
        /// </summary>
        public long FileSize { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when the file was created.
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Gets or sets the current processing status of the file.
        /// </summary>
        public FileProcessingStatus Status { get; set; }

        /// <summary>
        /// Gets or sets the number of times this file has been retried after failure.
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when the file last failed processing.
        /// </summary>
        public DateTime? LastFailedAt { get; set; }

        /// <summary>
        /// Gets or sets the last error message when processing failed.
        /// </summary>
        public string? LastError { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when processing started.
        /// </summary>
        public DateTime? ProcessingStartTime { get; set; }

        /// <summary>
        /// Gets or sets the earliest timestamp when this file is available for processing again.
        /// Used for delayed retry after failures.
        /// </summary>
        public DateTime? AvailableForProcessingAt { get; set; }

        /// <summary>
        /// Gets or sets the original file name (e.g., "invoice.pdf").
        /// This is the file name provided by the user when uploading or importing the file.
        /// </summary>
        public string? OriginalFileName { get; set; }

        /// <summary>
        /// Gets or sets the file extension (e.g., ".pdf", ".docx").
        /// Extracted from the original file name and applied to the physical file name.
        /// </summary>
        public string? FileExtension { get; set; }

        /// <summary>
        /// Gets or sets additional metadata as key-value pairs.
        /// </summary>
        public System.Collections.Generic.Dictionary<string, string>? Metadata { get; set; }
    }
}
