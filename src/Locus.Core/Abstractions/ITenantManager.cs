using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Manages tenant lifecycle and status in the multi-tenant storage system.
    /// </summary>
    public interface ITenantManager
    {
        /// <summary>
        /// Gets the tenant context for the specified tenant ID.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>The tenant context.</returns>
        Task<ITenantContext> GetTenantAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Checks whether a tenant is enabled and can perform operations.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>True if the tenant is enabled; otherwise, false.</returns>
        Task<bool> IsTenantEnabledAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Enables a tenant, allowing all operations.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task EnableTenantAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Disables a tenant, rejecting all operations.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task DisableTenantAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Creates a new tenant with the specified ID.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="ct">Cancellation token.</param>
        Task CreateTenantAsync(string tenantId, CancellationToken ct);

        /// <summary>
        /// Gets all tenants in the system.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>A collection of all tenant contexts.</returns>
        Task<IEnumerable<ITenantContext>> GetAllTenantsAsync(CancellationToken ct);
    }
}
