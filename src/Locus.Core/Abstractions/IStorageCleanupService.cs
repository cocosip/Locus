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
        Task CleanupEmptyDirectoriesAsync(ITenantContext tenant, CancellationToken ct);

        /// <summary>
        /// Cleans up empty directories for the specified tenant ID.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupEmptyDirectoriesAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Cleans up empty directories for all tenants.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupAllEmptyDirectoriesAsync(CancellationToken ct);

        /// <summary>
        /// Cleans up completed file records older than the specified timespan.
        /// </summary>
        /// <param name="olderThan">The age threshold for cleanup.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupCompletedFileRecordsAsync(TimeSpan olderThan, CancellationToken ct);

        /// <summary>
        /// Cleans up permanently failed files older than the specified timespan.
        /// </summary>
        /// <param name="olderThan">The age threshold for cleanup.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupPermanentlyFailedFilesAsync(TimeSpan olderThan, CancellationToken ct);

        /// <summary>
        /// Cleans up orphaned files (physical files with no metadata) for the specified tenant.
        /// </summary>
        /// <param name="tenant">The tenant context.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupOrphanedFilesAsync(ITenantContext tenant, CancellationToken ct);

        /// <summary>
        /// Cleans up files that have been in processing state longer than the timeout threshold.
        /// </summary>
        /// <param name="timeout">The processing timeout threshold.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CleanupTimedOutProcessingFilesAsync(TimeSpan timeout, CancellationToken ct);

        /// <summary>
        /// Gets statistics about cleanup operations.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Cleanup statistics.</returns>
        Task<CleanupStatistics> GetCleanupStatisticsAsync(CancellationToken ct);

        /// <summary>
        /// Optimizes (shrinks) all LiteDB databases by rebuilding them.
        /// This reclaims space from deleted records and reduces file size.
        /// WARNING: This operation can be time-consuming for large databases.
        /// Should be run during maintenance windows.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Database optimization statistics.</returns>
        Task<DatabaseOptimizationResult> OptimizeDatabasesAsync(CancellationToken ct);
    }
}
