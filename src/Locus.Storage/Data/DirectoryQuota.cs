using System;
using LiteDB;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Directory quota entity for tracking file count limits per directory.
    /// </summary>
    public class DirectoryQuota
    {
        /// <summary>
        /// Gets or sets the directory path (used as unique identifier).
        /// </summary>
        [BsonId]
        public string DirectoryPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the current number of files in this directory.
        /// </summary>
        public int CurrentCount { get; set; }

        /// <summary>
        /// Gets or sets the maximum number of files allowed in this directory.
        /// 0 means no limit.
        /// </summary>
        public int MaxCount { get; set; }

        /// <summary>
        /// Gets or sets whether quota enforcement is enabled for this directory.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets the timestamp when this quota was last updated.
        /// </summary>
        public DateTime LastUpdated { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when this quota was created.
        /// </summary>
        public DateTime CreatedAt { get; set; }
    }
}
