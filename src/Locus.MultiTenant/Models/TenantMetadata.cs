using System;
using Locus.Core.Models;

namespace Locus.MultiTenant.Models
{
    /// <summary>
    /// Represents the metadata for a tenant, stored in JSON format.
    /// </summary>
    public class TenantMetadata
    {
        /// <summary>
        /// Gets or sets the unique tenant identifier.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant status.
        /// </summary>
        public TenantStatus Status { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when the tenant was created.
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when the tenant metadata was last updated.
        /// </summary>
        public DateTime UpdatedAt { get; set; }

        /// <summary>
        /// Gets or sets the storage root path for this tenant.
        /// </summary>
        public string StoragePath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets additional tenant properties.
        /// </summary>
        public string? Properties { get; set; }
    }
}
