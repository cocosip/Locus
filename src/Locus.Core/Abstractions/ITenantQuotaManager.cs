using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Manages file count quotas for tenants.
    /// Supports global default quota and per-tenant specific quotas.
    /// </summary>
    public interface ITenantQuotaManager
    {
        /// <summary>
        /// Checks if a tenant can add a new file without exceeding quota.
        /// Uses tenant-specific quota if set, otherwise uses global quota.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if the file can be added, false if quota would be exceeded.</returns>
        Task<bool> CanAddFileAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Increments the file count for a tenant.
        /// Throws exception if quota would be exceeded.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <exception cref="Core.Exceptions.TenantQuotaExceededException">Thrown when quota limit is reached.</exception>
        Task IncrementFileCountAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Decrements the file count for a tenant (called when deleting files).
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task DecrementFileCountAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Gets the current file count for a tenant.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The current number of files for the tenant.</returns>
        Task<int> GetFileCountAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Gets the effective quota limit for a tenant.
        /// Returns tenant-specific limit if set, otherwise returns global limit.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The maximum file count (0 means unlimited).</returns>
        Task<int> GetEffectiveLimitAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Sets a tenant-specific quota limit (overrides global quota for this tenant).
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="maxFiles">The maximum number of files allowed (0 means unlimited).</param>
        /// <param name="ct">Cancellation token.</param>
        Task SetTenantLimitAsync(string tenantId, int maxFiles, CancellationToken ct);

        /// <summary>
        /// Removes tenant-specific quota limit (tenant will use global quota).
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task RemoveTenantLimitAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Sets the global default quota limit for all tenants (unless they have specific limits).
        /// </summary>
        /// <param name="maxFiles">The maximum number of files allowed (0 means unlimited).</param>
        /// <param name="ct">Cancellation token.</param>
        Task SetGlobalLimitAsync(int maxFiles, CancellationToken ct);

        /// <summary>
        /// Gets the global default quota limit.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The global maximum file count (0 means unlimited).</returns>
        Task<int> GetGlobalLimitAsync(CancellationToken ct);
    }
}

