using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when an operation is attempted on a disabled tenant.
    /// </summary>
    public class TenantDisabledException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TenantDisabledException"/> class.
        /// </summary>
        public TenantDisabledException()
            : base("The tenant is disabled and cannot perform operations.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantDisabledException"/> class with a tenant ID.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        public TenantDisabledException(string tenantId)
            : base($"The tenant '{tenantId}' is disabled and cannot perform operations.")
        {
            TenantId = tenantId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantDisabledException"/> class with a custom message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="tenantId">The tenant identifier.</param>
        public TenantDisabledException(string message, string tenantId)
            : base(message)
        {
            TenantId = tenantId;
        }

        /// <summary>
        /// Gets the tenant identifier associated with this exception.
        /// </summary>
        public string? TenantId { get; }
    }
}
