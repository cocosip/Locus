using System;
using System.IO.Abstractions;
using System.Linq;
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
                return new MetadataRepository(fileSystem, logger, options.MetadataDirectory, options.LiteDB);
            });

            services.AddSingleton(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<DirectoryQuotaRepository>>();
                return new DirectoryQuotaRepository(fileSystem, logger, options.QuotaDirectory, options.LiteDB);
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

            // Register StoragePool as its concrete type so StorageVolumeInitializationService
            // can call AddVolumeAsync directly. IStoragePool forwards to the same instance.
            services.AddSingleton<StoragePool>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var tenantQuotaManager = sp.GetRequiredService<ITenantQuotaManager>();
                var tenantManager = sp.GetRequiredService<ITenantManager>();
                var fileScheduler = sp.GetRequiredService<IFileScheduler>();
                var logger = sp.GetRequiredService<ILogger<StoragePool>>();

                // Volumes are NOT mounted here. StorageVolumeInitializationService mounts
                // them asynchronously in StartAsync, before requests are accepted.
                return new StoragePool(metadataRepo, tenantQuotaManager, tenantManager, fileScheduler, logger);
            });
            services.AddSingleton<IStoragePool>(sp => sp.GetRequiredService<StoragePool>());

            // Register StorageCleanupService as its concrete type so volumes can be registered
            // by StorageVolumeInitializationService. IStorageCleanupService forwards to the same instance.
            services.AddSingleton<StorageCleanupService>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var quotaRepo = sp.GetRequiredService<DirectoryQuotaRepository>();
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<StorageCleanupService>>();

                // Volumes are registered by StorageVolumeInitializationService after async mount.
                return new StorageCleanupService(
                    metadataRepo,
                    quotaRepo,
                    fileSystem,
                    logger,
                    options.MetadataDirectory,
                    options.QuotaDirectory);
            });
            services.AddSingleton<IStorageCleanupService>(sp => sp.GetRequiredService<StorageCleanupService>());

            // Mount volumes asynchronously at startup to avoid blocking Thread.Sleep calls.
            services.AddHostedService(sp => new StorageVolumeInitializationService(
                sp.GetRequiredService<StoragePool>(),
                sp.GetRequiredService<StorageCleanupService>(),
                sp.GetRequiredService<IFileSystem>(),
                sp.GetRequiredService<ILogger<StorageVolumeInitializationService>>(),
                options.Volumes,
                sp));

            // Register database recovery service
            services.AddSingleton<IDatabaseRecoveryService, Locus.Storage.Data.DatabaseRecoveryService>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var quotaRepo = sp.GetRequiredService<DirectoryQuotaRepository>();
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<Locus.Storage.Data.DatabaseRecoveryService>>();

                return new Locus.Storage.Data.DatabaseRecoveryService(
                    metadataRepo,
                    quotaRepo,
                    fileSystem,
                    logger,
                    options.MetadataDirectory,
                    options.QuotaDirectory);
            });

            // Register background cleanup service if enabled
            if (options.EnableBackgroundCleanup)
            {
                services.AddSingleton(options.CleanupOptions);
                services.AddHostedService<BackgroundCleanupService>();
            }

            // Register database health check service if enabled
            if (options.EnableDatabaseHealthCheck)
            {
                var volumePaths = options.Volumes.Select(v => v.MountPath).ToList();
                services.AddHostedService(sp => new DatabaseHealthCheckService(
                    sp.GetRequiredService<IDatabaseRecoveryService>(),
                    sp.GetRequiredService<IFileSystem>(),
                    sp.GetRequiredService<ILogger<DatabaseHealthCheckService>>(),
                    options.MetadataDirectory,
                    volumePaths,
                    options.AutoRecoverCorruptedDatabasesOnStartup,
                    options.FailFastOnStartupRecoveryFailure));
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
