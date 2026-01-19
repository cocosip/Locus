using System;
using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// Simplified root configuration for file watching.
    /// Automatically generates watcher configurations based on directory structure.
    /// </summary>
    public class FileWatcherRootConfiguration
    {
        /// <summary>
        /// Gets or sets the root directory path to monitor.
        /// </summary>
        public string RootPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets whether to use multi-tenant mode.
        /// When true, each immediate subdirectory under RootPath is treated as a tenant directory.
        /// When false, only the RootPath itself is monitored.
        /// Default is true.
        /// </summary>
        public bool MultiTenantMode { get; set; } = true;

        /// <summary>
        /// Gets or sets whether file watching is enabled.
        /// Default is true.
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
        /// Default is 4.
        /// </summary>
        public int MaxConcurrentImports { get; set; } = 4;

        /// <summary>
        /// Gets or sets whether to automatically discover new tenant directories.
        /// Only applicable when MultiTenantMode is true.
        /// Default is true.
        /// </summary>
        public bool AutoDiscoverTenants { get; set; } = true;

        /// <summary>
        /// Gets or sets the interval for discovering new tenant directories.
        /// Only applicable when AutoDiscoverTenants is true.
        /// Default is 5 minutes.
        /// </summary>
        public TimeSpan TenantDiscoveryInterval { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets whether to automatically create tenant subdirectories in multi-tenant mode.
        /// When enabled, a subdirectory will be created for each existing tenant in the system.
        /// Only applicable when MultiTenantMode is true.
        /// Default is false.
        /// </summary>
        public bool AutoCreateTenantDirectories { get; set; } = false;
    }
}
