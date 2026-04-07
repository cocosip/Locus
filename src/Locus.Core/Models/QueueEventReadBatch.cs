using System;
using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents a bounded batch of queue journal records read from a tenant log.
    /// </summary>
    public sealed class QueueEventReadBatch
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueueEventReadBatch"/> class.
        /// </summary>
        public QueueEventReadBatch(IReadOnlyList<QueueEventRecord> records, long nextOffset, bool reachedEndOfFile)
        {
            Records = records ?? throw new ArgumentNullException(nameof(records));
            NextOffset = nextOffset;
            ReachedEndOfFile = reachedEndOfFile;
        }

        /// <summary>
        /// Gets the records returned for the read.
        /// </summary>
        public IReadOnlyList<QueueEventRecord> Records { get; }

        /// <summary>
        /// Gets the next byte offset that should be used for a subsequent read.
        /// </summary>
        public long NextOffset { get; }

        /// <summary>
        /// Gets a value indicating whether the read reached the current end of file.
        /// </summary>
        public bool ReachedEndOfFile { get; }
    }
}
