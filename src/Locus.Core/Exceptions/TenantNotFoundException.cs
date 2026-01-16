using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when a tenant is not found in the system.
    /// </summary>
    public class TenantNotFoundException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TenantNotFoundException"/> class.
        /// </summary>
        public TenantNotFoundException()
            : base("The specified tenant was not found.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantNotFoundException"/> class with a tenant ID.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        public TenantNotFoundException(string tenantId)
            : base($"The tenant '{tenantId}' was not found.")
        {
            TenantId = tenantId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantNotFoundException"/> class with a custom message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="tenantId">The tenant identifier.</param>
        public TenantNotFoundException(string message, string tenantId)
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
