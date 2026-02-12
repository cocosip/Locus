using System;
using Locus.Core.Models;
using Microsoft.Extensions.DependencyInjection;

namespace Locus
{
    /// <summary>
    /// Builder for configuring Locus storage system with a fluent API.
    /// </summary>
    public class LocusBuilder
    {
        private readonly IServiceCollection _services;
        private readonly LocusOptions _options;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocusBuilder"/> class.
        /// </summary>
        /// <param name="services">The service collection.</param>
        public LocusBuilder(IServiceCollection services)
        {
            _services = services ?? throw new ArgumentNullException(nameof(services));
            _options = new LocusOptions();
        }

        /// <summary>
        /// Adds a local file system storage volume.
        /// </summary>
        /// <param name="volumeId">The unique volume identifier.</param>
        /// <param name="mountPath">The mount path for the volume.</param>
        /// <param name="shardingDepth">The sharding depth (0-3). Default: 2.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder AddLocalVolume(string volumeId, string mountPath, int shardingDepth = 2)
        {
            if (string.IsNullOrWhiteSpace(volumeId))
                throw new ArgumentException("Volume ID cannot be empty", nameof(volumeId));

            if (string.IsNullOrWhiteSpace(mountPath))
                throw new ArgumentException("Mount path cannot be empty", nameof(mountPath));

            if (shardingDepth < 0 || shardingDepth > 3)
                throw new ArgumentException("Sharding depth must be between 0 and 3", nameof(shardingDepth));

            _options.Volumes.Add(new VolumeConfiguration
            {
                VolumeId = volumeId,
                MountPath = mountPath,
                VolumeType = "LocalFileSystem",
                ShardingDepth = shardingDepth
            });

            return this;
        }

        /// <summary>
        /// Adds a storage volume with custom configuration.
        /// </summary>
        /// <param name="configure">Configuration action for the volume.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder AddVolume(Action<VolumeConfiguration> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            var volume = new VolumeConfiguration();
            configure(volume);
            volume.Validate();
            _options.Volumes.Add(volume);
            return this;
        }

        /// <summary>
        /// Configures the metadata storage directory.
        /// </summary>
        /// <param name="directory">The directory path.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithMetadataDirectory(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentException("Directory cannot be empty", nameof(directory));

            _options.MetadataDirectory = directory;
            return this;
        }

        /// <summary>
        /// Configures the quota storage directory.
        /// </summary>
        /// <param name="directory">The directory path.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithQuotaDirectory(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentException("Directory cannot be empty", nameof(directory));

            _options.QuotaDirectory = directory;
            return this;
        }

        /// <summary>
        /// Configures the file retry policy.
        /// </summary>
        /// <param name="configure">Configuration action for retry policy.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithRetryPolicy(Action<FileRetryPolicy> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            configure(_options.RetryPolicy);
            return this;
        }

        /// <summary>
        /// Configures cleanup options.
        /// </summary>
        /// <param name="configure">Configuration action for cleanup options.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithCleanupOptions(Action<Storage.CleanupOptions> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            configure(_options.CleanupOptions);
            return this;
        }

        /// <summary>
        /// Enables the background cleanup service.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder EnableBackgroundCleanup()
        {
            _options.EnableBackgroundCleanup = true;
            return this;
        }

        /// <summary>
        /// Disables the background cleanup service.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder DisableBackgroundCleanup()
        {
            _options.EnableBackgroundCleanup = false;
            return this;
        }

        /// <summary>
        /// Enables the database health check on startup.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder EnableDatabaseHealthCheck()
        {
            _options.EnableDatabaseHealthCheck = true;
            return this;
        }

        /// <summary>
        /// Disables the database health check on startup.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder DisableDatabaseHealthCheck()
        {
            _options.EnableDatabaseHealthCheck = false;
            return this;
        }

        /// <summary>
        /// Enables automatic rebuild of corrupted databases during startup health check.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder EnableStartupAutoRecovery()
        {
            _options.AutoRecoverCorruptedDatabasesOnStartup = true;
            return this;
        }

        /// <summary>
        /// Disables automatic rebuild of corrupted databases during startup health check.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder DisableStartupAutoRecovery()
        {
            _options.AutoRecoverCorruptedDatabasesOnStartup = false;
            return this;
        }

        /// <summary>
        /// Enables fail-fast startup behavior when automatic database recovery fails.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder EnableFailFastOnStartupRecoveryFailure()
        {
            _options.FailFastOnStartupRecoveryFailure = true;
            return this;
        }

        /// <summary>
        /// Disables fail-fast startup behavior when automatic database recovery fails.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder DisableFailFastOnStartupRecoveryFailure()
        {
            _options.FailFastOnStartupRecoveryFailure = false;
            return this;
        }

        /// <summary>
        /// Adds a pre-configured tenant to initialize on startup.
        /// </summary>
        /// <param name="tenantId">The tenant identifier.</param>
        /// <param name="quota">Optional file count quota (null = use default, 0 = unlimited).</param>
        /// <param name="enabled">Whether the tenant is enabled. Default: true.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder AddTenant(string tenantId, int? quota = null, bool enabled = true)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (quota.HasValue && quota.Value < 0)
                throw new ArgumentException("Quota cannot be negative", nameof(quota));

            _options.Tenants.Add(new TenantConfiguration
            {
                TenantId = tenantId,
                Quota = quota,
                Enabled = enabled
            });

            return this;
        }

        /// <summary>
        /// Adds a tenant with custom configuration.
        /// </summary>
        /// <param name="configure">Configuration action for the tenant.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder AddTenant(Action<TenantConfiguration> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            var tenant = new TenantConfiguration();
            configure(tenant);
            tenant.Validate();
            _options.Tenants.Add(tenant);
            return this;
        }

        /// <summary>
        /// Enables automatic tenant creation on first use.
        /// When enabled, tenants will be created automatically when referenced in file operations.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder EnableAutoCreateTenants()
        {
            _options.AutoCreateTenants = true;
            return this;
        }

        /// <summary>
        /// Disables automatic tenant creation.
        /// When disabled, all tenants must be pre-configured or created manually.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder DisableAutoCreateTenants()
        {
            _options.AutoCreateTenants = false;
            return this;
        }

        /// <summary>
        /// Sets the default file count quota for all tenants.
        /// This is used for tenants that don't have a specific quota configured.
        /// </summary>
        /// <param name="quota">The default quota (0 = unlimited).</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithDefaultTenantQuota(int quota)
        {
            if (quota < 0)
                throw new ArgumentException("Quota cannot be negative", nameof(quota));

            _options.DefaultTenantQuota = quota;
            return this;
        }

        /// <summary>
        /// Adds a file watcher configuration.
        /// </summary>
        /// <param name="configure">Configuration action for the file watcher.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder AddFileWatcher(Action<FileWatcherConfiguration> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            var watcher = new FileWatcherConfiguration();
            configure(watcher);
            _options.FileWatchers.Add(watcher);
            return this;
        }

        /// <summary>
        /// Configures the file watcher configuration directory.
        /// This directory stores persistent file watcher configurations.
        /// </summary>
        /// <param name="directory">The directory path.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithFileWatcherConfigurationDirectory(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentException("Directory cannot be empty", nameof(directory));

            _options.FileWatcherConfigurationDirectory = directory;
            return this;
        }

        /// <summary>
        /// Builds and registers Locus services with the service collection.
        /// </summary>
        /// <returns>The service collection for further configuration.</returns>
        public IServiceCollection Build()
        {
            _services.AddLocus(_options);
            return _services;
        }
    }

    /// <summary>
    /// Extension methods for LocusBuilder.
    /// </summary>
    public static class LocusBuilderExtensions
    {
        /// <summary>
        /// Adds Locus services using a fluent builder API.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configure">Configuration action for the builder.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddLocus(
            this IServiceCollection services,
            Action<LocusBuilder> configure)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            var builder = new LocusBuilder(services);
            configure(builder);
            return builder.Build();
        }
    }
}
