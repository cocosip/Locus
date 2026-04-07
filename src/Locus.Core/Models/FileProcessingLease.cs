using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents the processing lease returned when a file is allocated to a worker.
    /// </summary>
    public sealed class FileProcessingLease
    {
        /// <summary>
        /// Gets or sets the tenant identifier that owns the leased file.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the leased file key.
        /// </summary>
        public string FileKey { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the UTC timestamp that identifies the active processing lease.
        /// </summary>
        public DateTime ProcessingStartTimeUtc { get; set; }
    }
}
