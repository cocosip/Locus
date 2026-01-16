using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when no pending files are available for processing.
    /// </summary>
    public class NoFilesAvailableException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NoFilesAvailableException"/> class.
        /// </summary>
        public NoFilesAvailableException()
            : base("No files are available for processing.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NoFilesAvailableException"/> class with a tenant ID.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        public NoFilesAvailableException(string tenantId)
            : base($"No files are available for processing for tenant '{tenantId}'.")
        {
            TenantId = tenantId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="NoFilesAvailableException"/> class with a custom message and tenant ID.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="tenantId">The tenant identifier.</param>
        public NoFilesAvailableException(string message, string tenantId)
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
