using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents the location and metadata of a file in the storage pool.
    /// </summary>
    public class FileLocation
    {
        /// <summary>
        /// Gets or sets the unique file key.
        /// </summary>
        public string FileKey { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant ID that owns this file.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the ID of the storage volume where the file is located.
        /// </summary>
        public string VolumeId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the physical path of the file on the storage volume.
        /// </summary>
        public string PhysicalPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the directory path where the file is stored.
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
        /// Gets or sets the current retry count for failed processing.
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// Gets or sets the timestamp of the last processing failure.
        /// </summary>
        public DateTime? LastFailedAt { get; set; }

        /// <summary>
        /// Gets or sets the error message from the last processing failure.
        /// </summary>
        public string? LastError { get; set; }
    }
}
