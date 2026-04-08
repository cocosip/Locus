using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Represents cleanup-oriented operations over projected queue metadata.
    /// This keeps cleanup workflows on a projection-facing surface instead of
    /// coupling them directly to MetadataRepository implementation details.
    /// </summary>
    public interface IQueueProjectionCleanupStore
    {
        /// <summary>
        /// Gets a bounded batch of projected files in Processing status that timed out before the cutoff.
        /// </summary>
        Task<IReadOnlyList<FileMetadata>> GetTimedOutProcessingFilesAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            CancellationToken ct = default);

        /// <summary>
        /// Resets a projected timed-out file back to Pending if the processing lease still matches.
        /// </summary>
        Task<bool> TryResetTimedOutFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            DateTime availableForProcessingAtUtc,
            CancellationToken ct = default);

        /// <summary>
        /// Gets a bounded batch of projected files in Completed status older than the cutoff.
        /// </summary>
        Task<IReadOnlyList<FileMetadata>> GetCompletedFilesOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default);

        /// <summary>
        /// Gets a bounded batch of projected files in DeleteRequested status older than the cutoff.
        /// </summary>
        Task<IReadOnlyList<FileMetadata>> GetDeleteRequestedFilesOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default);

        /// <summary>
        /// Marks a projected completed file as DeleteSucceeded if it still matches the expected completion time.
        /// </summary>
        Task<bool> TryMarkDeleteSucceededAsync(
            string tenantId,
            string fileKey,
            DateTime expectedCompletedAtUtc,
            DateTime deleteSucceededAtUtc,
            CancellationToken ct = default);

        /// <summary>
        /// Gets a bounded batch of projected files in PermanentlyFailed status older than the cutoff.
        /// </summary>
        Task<IReadOnlyList<FileMetadata>> GetPermanentlyFailedFilesOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default);

        /// <summary>
        /// Removes a projected permanently-failed file if it still matches the expected failure timestamp.
        /// </summary>
        Task<bool> TryRemovePermanentlyFailedFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            CancellationToken ct = default);

        /// <summary>
        /// Removes a projected completed file if it still matches the expected completion timestamp.
        /// </summary>
        Task<bool> TryRemoveCompletedFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedCompletedAtUtc,
            CancellationToken ct = default);
    }
}
