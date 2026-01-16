namespace Locus.Core.Models
{
    /// <summary>
    /// Represents the status of a tenant in the system.
    /// </summary>
    public enum TenantStatus
    {
        /// <summary>
        /// Tenant is enabled and can perform all operations.
        /// </summary>
        Enabled = 1,

        /// <summary>
        /// Tenant is disabled and all operations are rejected.
        /// </summary>
        Disabled = 2,

        /// <summary>
        /// Tenant is temporarily suspended.
        /// </summary>
        Suspended = 3
    }
}
