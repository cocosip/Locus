namespace Locus.Core.Models
{
    /// <summary>
    /// Represents the processing status of a file in the storage pool.
    /// </summary>
    public enum FileProcessingStatus
    {
        /// <summary>
        /// File is pending and available for processing.
        /// </summary>
        Pending = 0,

        /// <summary>
        /// File is currently being processed.
        /// </summary>
        Processing = 1,

        /// <summary>
        /// File processing has completed successfully.
        /// </summary>
        Completed = 2,

        /// <summary>
        /// File processing has failed but can be retried.
        /// </summary>
        Failed = 3,

        /// <summary>
        /// File processing has permanently failed after exceeding max retry count.
        /// </summary>
        PermanentlyFailed = 4
    }
}
