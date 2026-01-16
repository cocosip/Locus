using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents a metadata operation in the Write-Ahead Log.
    /// </summary>
    public class MetadataOperation
    {
        /// <summary>
        /// Gets or sets the operation type.
        /// </summary>
        public MetadataOperationType OperationType { get; set; }

        /// <summary>
        /// Gets or sets the timestamp of the operation.
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the file key affected by this operation.
        /// </summary>
        public string? FileKey { get; set; }

        /// <summary>
        /// Gets or sets the directory path affected by this operation.
        /// </summary>
        public string? DirectoryPath { get; set; }

        /// <summary>
        /// Gets or sets the file location data (for AddFile operations).
        /// </summary>
        public FileLocation? FileLocation { get; set; }

        /// <summary>
        /// Gets or sets the new status (for UpdateStatus operations).
        /// </summary>
        public FileProcessingStatus? NewStatus { get; set; }

        /// <summary>
        /// Gets or sets the retry count (for UpdateRetry operations).
        /// </summary>
        public int? RetryCount { get; set; }

        /// <summary>
        /// Gets or sets the error message (for UpdateRetry operations).
        /// </summary>
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// Gets or sets the quota delta (for directory quota operations).
        /// </summary>
        public int? QuotaDelta { get; set; }
    }

    /// <summary>
    /// Defines the types of metadata operations.
    /// </summary>
    public enum MetadataOperationType
    {
        /// <summary>
        /// Add a new file to the store.
        /// </summary>
        AddFile = 1,

        /// <summary>
        /// Update file status.
        /// </summary>
        UpdateStatus = 2,

        /// <summary>
        /// Update file retry information.
        /// </summary>
        UpdateRetry = 3,

        /// <summary>
        /// Remove a file from the store.
        /// </summary>
        RemoveFile = 4,

        /// <summary>
        /// Increment directory file count.
        /// </summary>
        IncrementQuota = 5,

        /// <summary>
        /// Decrement directory file count.
        /// </summary>
        DecrementQuota = 6
    }
}
