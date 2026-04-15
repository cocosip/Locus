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

        /// <summary>
        /// Gets or sets the number of observed directory-preparation phases.
        /// </summary>
        public long DirectoryPreparationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent preparing destination directories.
        /// </summary>
        public long DirectoryPreparationTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed destination-stream open phases.
        /// </summary>
        public long OpenStreamCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent opening destination streams.
        /// </summary>
        public long OpenStreamTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed write copy operations.
        /// </summary>
        public long CopyOperationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent in copy operations.
        /// </summary>
        public long CopyTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of seekable FileStream copy operations that used Stream.CopyTo.
        /// </summary>
        public long SynchronousSeekableFileCopyToOperationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent in seekable FileStream CopyTo operations.
        /// </summary>
        public long SynchronousSeekableFileCopyToTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of seekable copy operations that used the manual buffer loop.
        /// </summary>
        public long SynchronousSeekableFileLoopOperationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent in seekable manual buffer-loop copies.
        /// </summary>
        public long SynchronousSeekableFileLoopTicks { get; set; }

        /// <summary>
        /// Gets or sets the number of observed flush operations.
        /// </summary>
        public long FlushOperationCount { get; set; }

        /// <summary>
        /// Gets or sets the total stopwatch ticks spent in flush operations.
        /// </summary>
        public long FlushTicks { get; set; }
    }
}
