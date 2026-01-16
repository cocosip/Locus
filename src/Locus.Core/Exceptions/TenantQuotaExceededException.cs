using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when a tenant's file quota limit is exceeded.
    /// </summary>
    public class TenantQuotaExceededException : Exception
    {
        /// <summary>
        /// Gets the tenant identifier.
        /// </summary>
        public string TenantId { get; }

        /// <summary>
        /// Gets the current file count.
        /// </summary>
        public int CurrentCount { get; }

        /// <summary>
        /// Gets the maximum allowed file count.
        /// </summary>
        public int MaxCount { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaExceededException"/> class.
        /// </summary>
        public TenantQuotaExceededException(string tenantId, int currentCount, int maxCount)
            : base($"Tenant '{tenantId}' quota exceeded: {currentCount}/{maxCount} files")
        {
            TenantId = tenantId;
            CurrentCount = currentCount;
            MaxCount = maxCount;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaExceededException"/> class with a custom message.
        /// </summary>
        public TenantQuotaExceededException(string tenantId, int currentCount, int maxCount, string message)
            : base(message)
        {
            TenantId = tenantId;
            CurrentCount = currentCount;
            MaxCount = maxCount;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaExceededException"/> class with a custom message and inner exception.
        /// </summary>
        public TenantQuotaExceededException(string tenantId, int currentCount, int maxCount, string message, Exception innerException)
            : base(message, innerException)
        {
            TenantId = tenantId;
            CurrentCount = currentCount;
            MaxCount = maxCount;
        }
    }
}
