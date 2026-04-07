using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Provides low-rate maintenance operations for a tenant queue projection.
    /// </summary>
    public interface IQueueProjectionMaintenanceService
    {
        /// <summary>
        /// Gets the current projection state for a tenant journal.
        /// </summary>
        Task<QueueProjectionTenantState> GetTenantStateAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Replays the tenant journal from the current cursor to the current end of file.
        /// </summary>
        Task<QueueProjectionTenantState> ReplayTenantAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Creates or refreshes the current projection snapshot for a tenant.
        /// </summary>
        Task<QueueProjectionTenantState> SnapshotTenantAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Rebuilds the tenant projection by clearing projected metadata, resetting the cursor,
        /// restoring the latest snapshot when available, replaying the remaining journal tail,
        /// and reconciling quota counters.
        /// </summary>
        Task<QueueProjectionTenantState> RebuildTenantAsync(string tenantId, CancellationToken ct = default);
    }
}
