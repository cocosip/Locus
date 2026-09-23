using System;

namespace Locus.Core.Models
{
    /// <summary>Defines the source action to perform after a successful import.</summary>
    public enum SourceCleanupJobAction
    {
        /// <summary>Delete the source file after import.</summary>
        Delete = 1,
        /// <summary>Move the source file after import.</summary>
        Move = 2,
        /// <summary>Keep the source file and retain the active job as a suppression marker.</summary>
        Keep = 3
    }

    /// <summary>Defines the durable lifecycle state of a source cleanup job.</summary>
    public enum SourceCleanupJobState
    {
        /// <summary>The cleanup action has not been attempted.</summary>
        Pending = 1,
        /// <summary>The cleanup action failed and is waiting for another attempt.</summary>
        Retrying = 2,
        /// <summary>Cleanup retries are exhausted and the source awaits failure-directory move.</summary>
        MovePending = 3,
        /// <summary>Cleanup retries are exhausted and no further action is scheduled.</summary>
        Failed = 4,
        /// <summary>The source import has been reserved but is not yet ready for cleanup.</summary>
        Importing = 5
    }

    /// <summary>
    /// Durable work item for the source file operation that follows a successful import.
    /// </summary>
    public sealed class SourceCleanupJob
    {
        /// <summary>Gets or sets the database identifier.</summary>
        public long Id { get; set; }

        /// <summary>Gets or sets the watcher identifier that created the job.</summary>
        public string WatcherId { get; set; } = string.Empty;

        /// <summary>Gets or sets the tenant identifier associated with the import.</summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>Gets or sets the source file path.</summary>
        public string SourcePath { get; set; } = string.Empty;

        /// <summary>Gets or sets the source fingerprint captured after import.</summary>
        public string Fingerprint { get; set; } = string.Empty;

        /// <summary>Gets or sets the Locus file key produced by the import.</summary>
        public string FileKey { get; set; } = string.Empty;

        /// <summary>Gets or sets the post-import source action.</summary>
        public SourceCleanupJobAction Action { get; set; }

        /// <summary>Gets or sets the destination path for a move action.</summary>
        public string? MoveTargetPath { get; set; }

        /// <summary>Gets or sets the directory used after cleanup retries are exhausted.</summary>
        public string? FailureDirectory { get; set; }

        /// <summary>Gets or sets the maximum number of cleanup attempts.</summary>
        public int MaxAttempts { get; set; } = 5;

        /// <summary>Gets or sets the initial cleanup retry delay.</summary>
        public TimeSpan RetryInitialDelay { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>Gets or sets the maximum cleanup retry delay.</summary>
        public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>Gets or sets the number of cleanup attempts already made.</summary>
        public int AttemptCount { get; set; }

        /// <summary>Gets or sets the durable cleanup state.</summary>
        public SourceCleanupJobState State { get; set; } = SourceCleanupJobState.Pending;

        /// <summary>Gets or sets the next eligible attempt time in UTC.</summary>
        public DateTime? NextAttemptUtc { get; set; }

        /// <summary>Gets or sets the most recent cleanup error.</summary>
        public string? LastError { get; set; }

        /// <summary>Gets or sets the creation time in UTC.</summary>
        public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

        /// <summary>Gets or sets the last update time in UTC.</summary>
        public DateTime UpdatedAtUtc { get; set; } = DateTime.UtcNow;

        /// <summary>Gets or sets the lease expiration time in UTC.</summary>
        public DateTime? LeaseUntilUtc { get; set; }
    }
}
