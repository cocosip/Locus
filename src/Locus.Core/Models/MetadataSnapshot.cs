using System;
using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Represents a snapshot of the entire metadata store state.
    /// </summary>
    public class MetadataSnapshot
    {
        /// <summary>
        /// Gets or sets the snapshot timestamp.
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the snapshot version number.
        /// </summary>
        public long Version { get; set; }

        /// <summary>
        /// Gets or sets all file locations in the system.
        /// </summary>
        public Dictionary<string, FileLocation> Files { get; set; } = new Dictionary<string, FileLocation>();

        /// <summary>
        /// Gets or sets the directory file counts.
        /// </summary>
        public Dictionary<string, int> DirectoryQuotas { get; set; } = new Dictionary<string, int>();

        /// <summary>
        /// Gets or sets the pending file queue per tenant.
        /// </summary>
        public Dictionary<string, List<string>> PendingFilesByTenant { get; set; } = new Dictionary<string, List<string>>();
    }
}
