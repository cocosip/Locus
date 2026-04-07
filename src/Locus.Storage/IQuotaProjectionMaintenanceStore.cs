using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Represents maintenance and recovery operations over projected quota storage.
    /// </summary>
    public interface IQuotaProjectionMaintenanceStore
    {
        /// <summary>
        /// Begins an exclusive quota database rebuild for a tenant.
        /// </summary>
        Task<IDatabaseRebuildLockHandle> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Force-increments a projected quota count.
        /// </summary>
        Task ForceIncrementAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Decrements a projected quota count.
        /// </summary>
        Task DecrementAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Gets all quota rows for a tenant.
        /// </summary>
        Task<IReadOnlyList<DirectoryQuota>> GetQuotaRowsAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Removes a quota row.
        /// </summary>
        Task<bool> RemoveQuotaAsync(string tenantId, string directoryPath, CancellationToken ct = default);

        /// <summary>
        /// Persists a projected quota count.
        /// </summary>
        Task SetProjectedCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default);

        /// <summary>
        /// Persists a projected quota count during rebuild.
        /// </summary>
        Task SetProjectedCountForRebuildAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default);

        /// <summary>
        /// Optimizes a tenant quota database and returns size statistics.
        /// </summary>
        Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Lists tenant identifiers that have quota state.
        /// </summary>
        Task<IReadOnlyList<string>> GetTenantIdsAsync(CancellationToken ct = default);
    }
}
