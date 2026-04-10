namespace Locus.Storage
{
    /// <summary>
    /// Controls when queued journal append requests are acknowledged to callers.
    /// </summary>
    public enum QueueEventJournalAckMode
    {
        /// <summary>
        /// Acknowledge only after the current micro-batch has been written and flushed.
        /// </summary>
        Durable = 0,

        /// <summary>
        /// Acknowledge after the current micro-batch has been written, while flush is
        /// deferred to a short batching window.
        /// </summary>
        Balanced = 1,

        /// <summary>
        /// Acknowledge immediately after enqueueing, before the background writer flushes.
        /// </summary>
        Async = 2,
    }
}
