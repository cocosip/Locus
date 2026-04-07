using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Service for recovering and rebuilding corrupted SQLite databases.
    /// </summary>
    public interface IDatabaseRecoveryService
    {
        /// <summary>
        /// Checks if a database file is corrupted.
        /// </summary>
        /// <param name="dbPath">Path to the database file.</param>
        /// <returns>True if corrupted, false otherwise.</returns>
        bool IsDatabaseCorrupted(string dbPath);

        /// <summary>
        /// Rebuilds a corrupted metadata database for a specific tenant.
        /// Implementations may prefer queue-journal recovery and fall back to physical file scanning.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="volumePaths">List of volume mount paths to scan.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the rebuild operation.</returns>
        Task<DatabaseRebuildResult> RebuildMetadataDatabaseAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct = default);

        /// <summary>
        /// Rebuilds a corrupted quota database for a specific tenant.
        /// Implementations may prefer projected metadata reconciliation and fall back to directory scanning.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="volumePaths">List of volume mount paths to scan.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Statistics about the rebuild operation.</returns>
        Task<DatabaseRebuildResult> RebuildQuotaDatabaseAsync(
            string tenantId,
            IEnumerable<string> volumePaths,
            CancellationToken ct = default);

        /// <summary>
        /// Checks all databases for corruption and returns a health report.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Health report for all databases.</returns>
        Task<DatabaseHealthReport> CheckAllDatabasesAsync(CancellationToken ct = default);
    }
}
