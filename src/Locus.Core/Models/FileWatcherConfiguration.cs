using System;
using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Configuration for monitoring a directory and importing files into the storage pool.
    /// </summary>
    public class FileWatcherConfiguration
    {
        /// <summary>
        /// Gets or sets the unique identifier for this watcher configuration.
        /// </summary>
        public string WatcherId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the tenant ID that this watcher belongs to.
        /// If MultiTenantMode is true, this should be empty and tenant IDs will be inferred from subdirectory names.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether to use multi-tenant mode.
        /// In multi-tenant mode, the WatchPath's immediate subdirectories are treated as tenant IDs.
        /// For example: /watch-root/tenant-001/files/ → tenant-001
        /// Default is false (single-tenant mode).
        /// </summary>
        public bool MultiTenantMode { get; set; } = false;

        /// <summary>
        /// Gets or sets whether to automatically create tenant subdirectories in multi-tenant mode.
        /// When enabled, the watcher will create a subdirectory for each existing tenant in the system.
        /// Only applies when MultiTenantMode is true.
        /// Default is false.
        /// </summary>
        public bool AutoCreateTenantDirectories { get; set; } = false;

        /// <summary>
        /// Gets or sets the root directory path to monitor.
        /// This directory and all subdirectories will be scanned recursively.
        /// </summary>
        public string WatchPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether this watcher is enabled.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to scan subdirectories recursively.
        /// Default is true.
        /// </summary>
        public bool IncludeSubdirectories { get; set; } = true;

        /// <summary>
        /// Gets or sets the file patterns to include (e.g., "*.txt", "*.pdf").
        /// If empty, all files are included.
        /// </summary>
        public List<string> FilePatterns { get; set; } = new List<string> { "*.*" };

        /// <summary>
        /// Gets or sets the action to take after a file is successfully imported.
        /// </summary>
        public PostImportAction PostImportAction { get; set; } = PostImportAction.Delete;

        /// <summary>
        /// Gets or sets the directory to move files to if PostImportAction is Move.
        /// </summary>
        public string? MoveToDirectory { get; set; }

        /// <summary>
        /// Gets or sets the polling interval for directory scanning.
        /// Default is 30 seconds.
        /// </summary>
        public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the maximum file size to import (in bytes).
        /// Files larger than this will be skipped. 0 means no limit.
        /// </summary>
        public long MaxFileSizeBytes { get; set; } = 0;

        /// <summary>
        /// Gets or sets the minimum file age before importing.
        /// This prevents importing files that are still being written.
        /// Default is 5 seconds.
        /// </summary>
        public TimeSpan MinFileAge { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Gets or sets the maximum number of concurrent file imports.
        /// Higher values improve throughput when processing many files.
        /// Set to 1 for sequential processing.
        /// Default is 4.
        /// </summary>
        public int MaxConcurrentImports { get; set; } = 4;

        /// <summary>
        /// Gets or sets the timestamp when this configuration was created.
        /// </summary>
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Gets or sets the timestamp when this configuration was last updated.
        /// </summary>
        public DateTime UpdatedAt { get; set; }
    }

    /// <summary>
    /// Defines actions to take after successfully importing a file.
    /// </summary>
    public enum PostImportAction
    {
        /// <summary>
        /// Delete the original file after import.
        /// </summary>
        Delete = 1,

        /// <summary>
        /// Move the file to another directory after import.
        /// </summary>
        Move = 2,

        /// <summary>
        /// Keep the original file in place after import.
        /// </summary>
        Keep = 3
    }
}
