using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents basic file information without physical location details.
    /// Used for returning file metadata to external callers.
    /// </summary>
    public class FileInfo
    {
        /// <summary>
        /// Gets or sets the unique file key.
        /// </summary>
        public string FileKey { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant identifier.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the file size in bytes.
        /// </summary>
        public long FileSize { get; set; }

        /// <summary>
        /// Gets or sets the creation timestamp (UTC).
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Gets or sets the current processing status of the file.
        /// </summary>
        public FileProcessingStatus Status { get; set; }

        /// <summary>
        /// Gets or sets the retry count for failed processing.
        /// </summary>
        public int RetryCount { get; set; }
    }
}
