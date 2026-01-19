using System;
using System.Collections.Generic;
using Locus.Core.Models;

namespace Locus
{
    /// <summary>
    /// Configuration options for the Locus storage system.
    /// </summary>
    public class LocusOptions
    {
        /// <summary>
        /// Gets or sets the directory where metadata will be stored.
        /// Default: "./locus-metadata"
        /// </summary>
        public string MetadataDirectory { get; set; } = "./locus-metadata";

        /// <summary>
        /// Gets or sets the directory where quota information will be stored.
        /// Default: "./locus-quota"
        /// </summary>
        public string QuotaDirectory { get; set; } = "./locus-quota";

        /// <summary>
        /// Gets or sets the file retry policy.
        /// </summary>
        public FileRetryPolicy RetryPolicy { get; set; } = new FileRetryPolicy
        {
            MaxRetryCount = 3,
            InitialRetryDelay = TimeSpan.FromSeconds(5),
            UseExponentialBackoff = true,
            MaxRetryDelay = TimeSpan.FromMinutes(5)
        };

        /// <summary>
        /// Gets the list of storage volumes to mount.
        /// </summary>
        public List<VolumeConfiguration> Volumes { get; } = [];

        /// <summary>
        /// Gets or sets the cleanup options.
        /// </summary>
        public Storage.CleanupOptions CleanupOptions { get; set; } = new Storage.CleanupOptions();

        /// <summary>
        /// Gets or sets whether to enable the background cleanup service.
        /// Default: true
        /// </summary>
        public bool EnableBackgroundCleanup { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to enable database health check on startup.
        /// When enabled, all databases will be checked for corruption on application startup.
        /// Corrupted databases will be logged with instructions for recovery.
        /// Default: true
        /// </summary>
        public bool EnableDatabaseHealthCheck { get; set; } = true;

        /// <summary>
        /// Gets the list of pre-configured tenants to initialize on startup.
        /// If empty, tenants can still be created dynamically if AutoCreateTenants is true.
        /// </summary>
        public List<TenantConfiguration> Tenants { get; } = [];

        /// <summary>
        /// Gets or sets whether to automatically create tenants on first use.
        /// If true, when a file operation is attempted for a non-existent tenant,
        /// the tenant will be automatically created with default settings.
        /// If false, tenants must be pre-configured or created manually before use.
        /// Default: true
        /// </summary>
        public bool AutoCreateTenants { get; set; } = true;

        /// <summary>
        /// Gets or sets the default file count quota for all tenants.
        /// This is used as the global quota limit for tenants that don't have a specific quota set.
        /// 0 = unlimited (default)
        /// </summary>
        public int DefaultTenantQuota { get; set; } = 0;

        /// <summary>
        /// Gets the list of file watcher configurations.
        /// File watchers automatically monitor directories and import files into the storage pool.
        /// </summary>
        public List<FileWatcherConfiguration> FileWatchers { get; } = [];

        /// <summary>
        /// Gets or sets the directory where file watcher configurations will be stored.
        /// Default: "./locus-watchers"
        /// </summary>
        public string FileWatcherConfigurationDirectory { get; set; } = "./locus-watchers";
    }

    /// <summary>
    /// Configuration for a storage volume.
    /// </summary>
    public class VolumeConfiguration
    {
        /// <summary>
        /// Gets or sets the unique volume identifier.
        /// </summary>
        public string VolumeId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the mount path for this volume.
        /// </summary>
        public string MountPath { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the volume type.
        /// Default: "LocalFileSystem"
        /// </summary>
        public string VolumeType { get; set; } = "LocalFileSystem";

        /// <summary>
        /// Gets or sets the sharding depth for automatic directory sharding.
        /// Uses 2-character hex format (00-FF) similar to FastDFS.
        /// 0 = No sharding (all files in tenant root)
        /// 1 = Single level (256 directories: 00-ff)
        /// 2 = Two levels (65,536 directories: 00/00 to ff/ff) - RECOMMENDED
        /// 3 = Three levels (16,777,216 directories: 00/00/00 to ff/ff/ff)
        /// Example with depth=2: {volume}/{tenant}/a1/b2/{fileKey}
        /// Default: 2
        /// </summary>
        public int ShardingDepth { get; set; } = 2;

        /// <summary>
        /// Validates the volume configuration.
        /// </summary>
        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(VolumeId))
                throw new InvalidOperationException("VolumeId cannot be empty");

            if (string.IsNullOrWhiteSpace(MountPath))
                throw new InvalidOperationException("MountPath cannot be empty");

            if (ShardingDepth < 0 || ShardingDepth > 3)
                throw new InvalidOperationException("ShardingDepth must be between 0 and 3");
        }
    }

    /// <summary>
    /// Configuration for a tenant.
    /// </summary>
    public class TenantConfiguration
    {
        /// <summary>
        /// Gets or sets the unique tenant identifier.
        /// </summary>
        public string TenantId { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the file count quota for this specific tenant.
        /// If null, the global DefaultTenantQuota will be used.
        /// If set to 0, this tenant has unlimited quota.
        /// </summary>
        public int? Quota { get; set; }

        /// <summary>
        /// Gets or sets whether this tenant is enabled.
        /// Default: true
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Validates the tenant configuration.
        /// </summary>
        public void Validate()
        {
            if (string.IsNullOrWhiteSpace(TenantId))
                throw new InvalidOperationException("TenantId cannot be empty");

            if (Quota.HasValue && Quota.Value < 0)
                throw new InvalidOperationException("Quota cannot be negative");
        }
    }
}
