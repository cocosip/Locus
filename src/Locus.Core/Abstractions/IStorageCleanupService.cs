using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Provides automatic cleanup services for the storage pool.
    /// </summary>
    public interface IStorageCleanupService
    {
        /// <summary>
        /// Cleans up empty directories for the specified tenant.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupEmptyDirectoriesAsync(ITenantContext tenant, CancellationToken ct = default);

        /// <summary>
        /// Cleans up empty directories for the specified tenant ID.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupEmptyDirectoriesAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Cleans up empty directories for all tenants.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupAllEmptyDirectoriesAsync(CancellationToken ct = default);

        /// <summary>
        /// Cleans up permanently failed files older than the specified timespan.
        /// </summary>
        /// <param name="olderThan">The age threshold for cleanup.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupPermanentlyFailedFilesAsync(TimeSpan olderThan, CancellationToken ct = default);

        /// <summary>
        /// Recovers orphaned files (physical files with no metadata) for the specified tenant
        /// by rebuilding their metadata entries and re-queuing them as Pending.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RecoverOrphanedFilesAsync(ITenantContext tenant, CancellationToken ct = default);

        /// <summary>
        /// Recovers orphaned files across all registered storage volumes by discovering tenant
        /// directories under each volume mount path and rebuilding missing metadata entries.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task RecoverAllOrphanedFilesAsync(CancellationToken ct = default);

        /// <summary>
        /// Reconciles quota counters for a single tenant by recomputing counts from metadata.
        /// This is a maintenance action and is not intended for the regular startup path.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task ReconcileQuotaCountsAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Reconciles quota counters across all known tenants by recomputing counts from metadata.
        /// This is a maintenance action and is not intended for the regular startup path.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task ReconcileAllQuotaCountsAsync(CancellationToken ct = default);

        /// <summary>
        /// Cleans up files that have been in processing state longer than the timeout threshold.
        /// </summary>
        /// <param name="timeout">The processing timeout threshold.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupTimedOutProcessingFilesAsync(TimeSpan timeout, CancellationToken ct = default);

        /// <summary>
        /// Performs timed-out and permanently-failed cleanup in a single pass over the
        /// metadata store, avoiding redundant per-status GetAllAsync calls.
        /// Pass null for any parameter to skip that cleanup category.
        /// </summary>
        /// <param name="processingTimeout">Reset Processing files older than this to Pending. Null to skip.</param>
        /// <param name="failedRetentionPeriod">Delete PermanentlyFailed files older than this. Null to skip.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupFilesByStatusAsync(
            TimeSpan? processingTimeout,
            TimeSpan? failedRetentionPeriod,
            CancellationToken ct = default);

        /// <summary>
        /// Gets statistics about cleanup operations.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Cleanup statistics.</returns>
        Task<CleanupStatistics> GetCleanupStatisticsAsync(CancellationToken ct = default);

        /// <summary>
        /// Optimizes (shrinks) all SQLite databases by running VACUUM.
        /// This reclaims space from deleted records and reduces file size.
        /// WARNING: This operation can be time-consuming for large databases.
        /// Should be run during maintenance windows.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Database optimization statistics.</returns>
        Task<DatabaseOptimizationResult> OptimizeDatabasesAsync(CancellationToken ct = default);

        /// <summary>
        /// Cleans up SQLite corruption backup files (*.corrupted.*) left over from database rebuild operations.
        /// These accumulate inside each tenant's subdirectory when a corrupted database is rebuilt.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Number of invalid database files removed and space freed in bytes.</returns>
        Task<(int FilesRemoved, long SpaceFreed)> CleanupInvalidDatabaseFilesAsync(CancellationToken ct = default);
    }
}
