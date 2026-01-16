using System;
using System.IO.Abstractions;
using Locus.Core.Abstractions;
using Locus.FileSystem;
using Locus.MultiTenant;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Locus
{
    /// <summary>
    /// Extension methods for configuring Locus services.
    /// </summary>
    public static class ServiceCollectionExtensions
    {
        /// <summary>
        /// Adds Locus file storage services to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configure">Configuration action for Locus options.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddLocus(
            this IServiceCollection services,
            Action<LocusOptions> configure)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            var options = new LocusOptions();
            configure(options);

            return services.AddLocus(options);
        }

        /// <summary>
        /// Adds Locus file storage services to the service collection.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="options">The Locus configuration options.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddLocus(
            this IServiceCollection services,
            LocusOptions options)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            if (options == null)
                throw new ArgumentNullException(nameof(options));

            // Validate options
            if (options.Volumes.Count == 0)
                throw new InvalidOperationException("At least one storage volume must be configured");

            foreach (var volume in options.Volumes)
            {
                volume.Validate();
            }

            foreach (var tenant in options.Tenants)
            {
                tenant.Validate();
            }

            // Register file system abstraction
            services.AddSingleton<IFileSystem, System.IO.Abstractions.FileSystem>();

            // Register repositories as singletons (they manage their own state)
            services.AddSingleton(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<MetadataRepository>>();
                return new MetadataRepository(fileSystem, logger, options.MetadataDirectory);
            });

            services.AddSingleton(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<DirectoryQuotaRepository>>();
                return new DirectoryQuotaRepository(fileSystem, logger, options.QuotaDirectory);
            });

            // Register tenant manager
            services.AddSingleton<ITenantManager>(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<TenantManager>>();
                return new TenantManager(fileSystem, logger, options.MetadataDirectory, autoCreateTenants: options.AutoCreateTenants);
            });

            // Register directory quota manager
            services.AddSingleton<IDirectoryQuotaManager>(sp =>
            {
                var repository = sp.GetRequiredService<DirectoryQuotaRepository>();
                var logger = sp.GetRequiredService<ILogger<DirectoryQuotaManager>>();
                return new DirectoryQuotaManager(repository, logger);
            });

            // Register tenant quota manager
            services.AddSingleton<ITenantQuotaManager>(sp =>
            {
                var repository = sp.GetRequiredService<DirectoryQuotaRepository>();
                var logger = sp.GetRequiredService<ILogger<TenantQuotaManager>>();
                return new TenantQuotaManager(repository, logger);
            });

            // Register file watcher
            services.AddSingleton<IFileWatcher>(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var storagePool = sp.GetRequiredService<IStoragePool>();
                var tenantManager = sp.GetRequiredService<ITenantManager>();
                var logger = sp.GetRequiredService<ILogger<FileWatcher>>();
                return new FileWatcher(fileSystem, storagePool, tenantManager, logger, options.FileWatcherConfigurationDirectory);
            });

            // Register file scheduler
            services.AddSingleton<IFileScheduler>(sp =>
            {
                var repository = sp.GetRequiredService<MetadataRepository>();
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<FileScheduler>>();
                return new FileScheduler(repository, fileSystem, logger, options.RetryPolicy);
            });

            // Register storage pool
            services.AddSingleton<IStoragePool>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var tenantQuotaManager = sp.GetRequiredService<ITenantQuotaManager>();
                var tenantManager = sp.GetRequiredService<ITenantManager>();
                var fileScheduler = sp.GetRequiredService<IFileScheduler>();
                var logger = sp.GetRequiredService<ILogger<StoragePool>>();

                var pool = new StoragePool(
                    metadataRepo,
                    tenantQuotaManager,
                    tenantManager,
                    fileScheduler,
                    logger);

                // Mount configured volumes
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                foreach (var volumeConfig in options.Volumes)
                {
                    var volume = CreateVolume(volumeConfig, fileSystem, sp);
                    pool.AddVolume(volume);
                }

                return pool;
            });

            // Register cleanup service
            services.AddSingleton<IStorageCleanupService>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var quotaRepo = sp.GetRequiredService<DirectoryQuotaRepository>();
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<StorageCleanupService>>();

                var cleanupService = new StorageCleanupService(metadataRepo, quotaRepo, fileSystem, logger);

                // Register volumes with cleanup service
                foreach (var volumeConfig in options.Volumes)
                {
                    var volume = CreateVolume(volumeConfig, fileSystem, sp);
                    cleanupService.RegisterVolume(volume);
                }

                return cleanupService;
            });

            // Register background cleanup service if enabled
            if (options.EnableBackgroundCleanup)
            {
                services.AddSingleton(options.CleanupOptions);
                services.AddHostedService<BackgroundCleanupService>();
            }

            // Store LocusOptions for runtime access (e.g., AutoCreateTenants)
            services.AddSingleton(options);

            // Initialize pre-configured tenants on startup
            services.AddHostedService(sp => new TenantInitializationService(
                sp.GetRequiredService<ITenantManager>(),
                sp.GetRequiredService<ITenantQuotaManager>(),
                sp.GetRequiredService<ILogger<TenantInitializationService>>(),
                options));

            // Initialize pre-configured file watchers on startup
            if (options.FileWatchers.Count > 0)
            {
                services.AddHostedService(sp => new FileWatcherInitializationService(
                    sp.GetRequiredService<IFileWatcher>(),
                    sp.GetRequiredService<ILogger<FileWatcherInitializationService>>(),
                    options));
            }

            return services;
        }

        /// <summary>
        /// Creates a storage volume based on configuration.
        /// </summary>
        private static IStorageVolume CreateVolume(
            VolumeConfiguration config,
            IFileSystem fileSystem,
            IServiceProvider serviceProvider)
        {
            switch (config.VolumeType.ToLowerInvariant())
            {
                case "localfilesystem":
                case "local":
                    var logger = serviceProvider.GetRequiredService<ILogger<LocalFileSystemVolume>>();
                    return new LocalFileSystemVolume(
                        fileSystem,
                        logger,
                        config.VolumeId,
                        config.MountPath,
                        config.ShardingDepth);

                default:
                    throw new NotSupportedException($"Volume type '{config.VolumeType}' is not supported");
            }
        }
    }
}
