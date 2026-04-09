namespace Locus.Core.Models
{
    /// <summary>
    /// Defines the durable queue event types emitted by the storage pipeline.
    /// </summary>
    public enum QueueEventType
    {
        /// <summary>
        /// A file has been durably accepted after physical write completion.
        /// </summary>
        Accepted = 1,

        /// <summary>
        /// A worker has acquired the file and started processing it.
        /// </summary>
        ProcessingStarted = 2,

        /// <summary>
        /// A worker has failed processing and the file was returned for retry or marked failed.
        /// </summary>
        ProcessingFailed = 3,

        /// <summary>
        /// Processing finished successfully and the file was marked completed.
        /// </summary>
        ProcessingCompleted = 4,

        /// <summary>
        /// A completed file was queued for low-rate physical deletion.
        /// </summary>
        DeleteRequested = 5,

        /// <summary>
        /// A completed file was deleted physically and can be removed from projections.
        /// </summary>
        DeleteSucceeded = 6,

        /// <summary>
        /// A processing lease timed out and the file was returned to pending state.
        /// </summary>
        ProcessingTimedOut = 7,

        /// <summary>
        /// A permanently failed file was moved to dead letter and removed from the active queue.
        /// </summary>
        DeadLettered = 8,
    }
}
