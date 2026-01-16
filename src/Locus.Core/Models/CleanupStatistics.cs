namespace Locus.Core.Models
{
    /// <summary>
    /// Statistics collected during cleanup operations.
    /// </summary>
    public class CleanupStatistics
    {
        /// <summary>
        /// Gets or sets the number of empty directories removed.
        /// </summary>
        public int EmptyDirectoriesRemoved { get; set; }

        /// <summary>
        /// Gets or sets the number of completed file records removed.
        /// </summary>
        public int CompletedRecordsRemoved { get; set; }

        /// <summary>
        /// Gets or sets the number of permanently failed files removed.
        /// </summary>
        public int PermanentlyFailedFilesRemoved { get; set; }

        /// <summary>
        /// Gets or sets the number of orphaned files removed.
        /// </summary>
        public int OrphanedFilesRemoved { get; set; }

        /// <summary>
        /// Gets or sets the number of timed-out files reset to pending status.
        /// </summary>
        public int TimedOutFilesReset { get; set; }

        /// <summary>
        /// Gets or sets the total disk space freed in bytes.
        /// </summary>
        public long SpaceFreed { get; set; }
    }
}
