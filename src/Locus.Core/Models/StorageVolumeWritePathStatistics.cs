namespace Locus.Core.Models
{
    /// <summary>
    /// Aggregated diagnostics for the observed write-source mix of a storage volume.
    /// </summary>
    public sealed class StorageVolumeWritePathStatistics
    {
        /// <summary>
        /// Gets or sets the total number of observed writes.
        /// </summary>
        public long TotalWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of writes whose source stream was a MemoryStream.
        /// </summary>
        public long MemoryStreamWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of writes whose MemoryStream exposed its underlying buffer.
        /// </summary>
        public long VisibleBufferWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of writes that used the direct MemoryStream fast path.
        /// </summary>
        public long DirectFastPathWrites { get; set; }

        /// <summary>
        /// Gets or sets the total bytes written through the direct MemoryStream fast path.
        /// </summary>
        public long DirectFastPathBytes { get; set; }

        /// <summary>
        /// Gets or sets the number of direct fast-path writes that used the synchronous small-write path.
        /// </summary>
        public long DirectFastPathSyncWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of direct fast-path writes that used the asynchronous write path.
        /// </summary>
        public long DirectFastPathAsyncWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of writes that used the synchronous seekable file-copy path.
        /// </summary>
        public long SynchronousSeekableFileWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of MemoryStream writes whose underlying buffer was not publicly visible.
        /// </summary>
        public long HiddenMemoryStreamWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of non-MemoryStream writes whose source stream was seekable.
        /// </summary>
        public long NonMemorySeekableWrites { get; set; }

        /// <summary>
        /// Gets or sets the number of non-seekable source writes.
        /// </summary>
        public long NonSeekableWrites { get; set; }
    }
}
