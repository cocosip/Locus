namespace Locus.Core.Models
{
    /// <summary>
    /// Aggregated diagnostics for the observed append/flush activity of a queue journal.
    /// </summary>
    public sealed class QueueJournalWritePathStatistics
    {
        /// <summary>
        /// Gets or sets the total number of append batches written.
        /// </summary>
        public long AppendBatchCount { get; set; }

        /// <summary>
        /// Gets or sets the number of append batches that contained exactly one record.
        /// </summary>
        public long SingleRecordAppendBatchCount { get; set; }

        /// <summary>
        /// Gets or sets the number of append batches that contained multiple records.
        /// </summary>
        public long MultiRecordAppendBatchCount { get; set; }

        /// <summary>
        /// Gets or sets the total number of appended records.
        /// </summary>
        public long AppendedRecordCount { get; set; }

        /// <summary>
        /// Gets or sets the total bytes appended to queue journals.
        /// </summary>
        public long AppendedBytes { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent writing append batches.
        /// </summary>
        public long AppendTicks { get; set; }

        /// <summary>
        /// Gets or sets the total number of append-stream flushes.
        /// </summary>
        public long FlushCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent flushing append streams.
        /// </summary>
        public long FlushTicks { get; set; }
    }
}
