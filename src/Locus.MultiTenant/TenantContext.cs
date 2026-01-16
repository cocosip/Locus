using Locus.Core.Abstractions;
using Locus.Core.Models;

namespace Locus.MultiTenant
{
    /// <summary>
    /// Represents a tenant's context in the multi-tenant storage system.
    /// </summary>
    public class TenantContext : ITenantContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TenantContext"/> class.
        /// </summary>
        /// <param name="tenantId">The unique tenant identifier.</param>
        /// <param name="status">The tenant status.</param>
        public TenantContext(string tenantId, TenantStatus status)
        {
            TenantId = tenantId;
            Status = status;
        }

        /// <inheritdoc/>
        public string TenantId { get; }

        /// <inheritdoc/>
        public TenantStatus Status { get; }
    }
}
