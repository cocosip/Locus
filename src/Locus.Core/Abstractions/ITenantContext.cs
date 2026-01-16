using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Represents the context for a tenant in the multi-tenant storage system.
    /// </summary>
    public interface ITenantContext
    {
        /// <summary>
        /// Gets the unique identifier for the tenant.
        /// </summary>
        string TenantId { get; }

        /// <summary>
        /// Gets the current status of the tenant.
        /// </summary>
        TenantStatus Status { get; }
    }
}
