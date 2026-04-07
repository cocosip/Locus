using System;
using System.IO.Abstractions;
using System.Linq;
using Locus.Core.Abstractions;
using Locus.FileSystem;
using Locus.MultiTenant;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Configuration;
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
        /// Adds Locus file storage services from configuration.
        /// Binds from section "Locus" by default.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configuration">Application configuration root.</param>
        /// <param name="sectionName">Configuration section name. Default: "Locus".</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddLocus(
            this IServiceCollection services,
            IConfiguration configuration,
            string sectionName = "Locus")
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var options = new LocusOptions();
            configuration.GetSection(sectionName).Bind(options);
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

            if (options.PersistenceDrainBatchSize != MetadataRepositoryOptions.DefaultDrainBatchSize
                && options.MetadataRepository.DrainBatchSize == MetadataRepositoryOptions.DefaultDrainBatchSize)
            {
                options.MetadataRepository.DrainBatchSize = options.PersistenceDrainBatchSize;
            }

            options.MetadataRepository.Validate();
            options.StoragePool.Validate();
            options.QueueEventJournal.Validate();

            // Register file system abstraction
            services.AddSingleton<IFileSystem, System.IO.Abstractions.FileSystem>();
            services.AddSingleton<StorageVolumeRegistry>();

            // Register repositories as singletons (they manage their own state)
            services.AddSingleton(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<MetadataRepository>>();
                return new MetadataRepository(
                    fileSystem,
                    logger,
                    options.MetadataDirectory,
                    options.Sqlite,
                    enableBackgroundPersistence: options.MetadataRepository.EnableBackgroundPersistence,
                    maxDrainBatchSize: options.MetadataRepository.DrainBatchSize,
                    persistenceQueueSoftMergeThresholdPercent: options.MetadataRepository.SoftMergeThresholdPercent,
                    maxPersistenceQueueSize: options.MetadataRepository.MaxQueueSize,
                    startupLoadBatchSize: options.MetadataRepository.StartupLoadBatchSize,
                    shutdownDrainTimeoutSeconds: options.MetadataRepository.ShutdownDrainTimeoutSeconds,
                    persistenceIntervalSeconds: options.MetadataRepository.PersistenceIntervalSeconds);
            });

            services.AddSingleton<IQueueProjectionStore>(sp =>
            {
                var repository = sp.GetRequiredService<MetadataRepository>();
                return new MetadataRepositoryQueueProjectionStore(repository);
            });

            services.AddSingleton<IQueueProjectionWriteStore>(sp =>
            {
                var repository = sp.GetRequiredService<MetadataRepository>();
                return new MetadataRepositoryQueueProjectionWriteStore(repository);
            });

            services.AddSingleton<IQueueProjectionCleanupStore>(sp =>
            {
                var repository = sp.GetRequiredService<MetadataRepository>();
                return new MetadataRepositoryQueueProjectionCleanupStore(repository);
            });

            services.AddSingleton<IMetadataProjectionMaintenanceStore>(sp =>
            {
                var repository = sp.GetRequiredService<MetadataRepository>();
                return new MetadataRepositoryProjectionMaintenanceStore(repository);
            });

            services.AddSingleton<IQuotaProjectionStore>(sp =>
            {
                var repository = sp.GetRequiredService<DirectoryQuotaRepository>();
                return new DirectoryQuotaRepositoryProjectionStore(repository);
            });

            services.AddSingleton<IQuotaProjectionMaintenanceStore>(sp =>
            {
                var repository = sp.GetRequiredService<DirectoryQuotaRepository>();
                return new DirectoryQuotaRepositoryProjectionMaintenanceStore(repository);
            });

            services.AddSingleton(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<DirectoryQuotaRepository>>();
                return new DirectoryQuotaRepository(fileSystem, logger, options.QuotaDirectory, options.Sqlite);
            });

            // Register tenant manager
            services.AddSingleton<ITenantManager>(sp =>
            {
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<TenantManager>>();
                return new TenantManager(fileSystem, logger, options.MetadataDirectory, autoCreateTenants: options.AutoCreateTenants);
            });

            services.AddSingleton<DirectoryQuotaManager>(sp =>
            {
                var quotaStore = sp.GetRequiredService<IQuotaProjectionStore>();
                var logger = sp.GetRequiredService<ILogger<DirectoryQuotaManager>>();
                return new DirectoryQuotaManager(quotaStore, logger);
            });
            services.AddSingleton<IDirectoryQuotaManager>(sp => sp.GetRequiredService<DirectoryQuotaManager>());

            services.AddSingleton<TenantQuotaManager>(sp =>
            {
                var quotaStore = sp.GetRequiredService<IQuotaProjectionStore>();
                var logger = sp.GetRequiredService<ILogger<TenantQuotaManager>>();
                return new TenantQuotaManager(quotaStore, logger);
            });
            services.AddSingleton<ITenantQuotaManager>(sp => sp.GetRequiredService<TenantQuotaManager>());

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
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<FileScheduler>>();
                var volumeRegistry = sp.GetRequiredService<StorageVolumeRegistry>();
                var tenantQuotaManager = sp.GetRequiredService<ITenantQuotaManager>();
                var directoryQuotaManager = sp.GetRequiredService<IDirectoryQuotaManager>();
                var queueEventJournal = sp.GetService<IQueueEventJournal>();
                var projectionStore = sp.GetRequiredService<IQueueProjectionStore>();
                return new FileScheduler(
                    projectionStore,
                    fileSystem,
                    logger,
                    options.RetryPolicy,
                    volumeRegistry,
                    tenantQuotaManager,
                    directoryQuotaManager,
                    queueEventJournal);
            });

            // Register StoragePool as its concrete type so StorageVolumeInitializationService
            // can call AddVolumeAsync directly. IStoragePool forwards to the same instance.
            services.AddSingleton<StoragePool>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var tenantQuotaManager = sp.GetRequiredService<ITenantQuotaManager>();
                var directoryQuotaManager = sp.GetRequiredService<IDirectoryQuotaManager>();
                var tenantManager = sp.GetRequiredService<ITenantManager>();
                var fileScheduler = sp.GetRequiredService<IFileScheduler>();
                var logger = sp.GetRequiredService<ILogger<StoragePool>>();
                var volumeRegistry = sp.GetRequiredService<StorageVolumeRegistry>();
                var queueEventJournal = sp.GetService<IQueueEventJournal>();
                var projectionStore = sp.GetRequiredService<IQueueProjectionStore>();
                var projectionWriteStore = sp.GetRequiredService<IQueueProjectionWriteStore>();

                // Volumes are NOT mounted here. StorageVolumeInitializationService mounts
                // them asynchronously in StartAsync, before requests are accepted.
                return new StoragePool(
                    metadataRepo,
                    tenantQuotaManager,
                    directoryQuotaManager,
                    tenantManager,
                    fileScheduler,
                    logger,
                    options.StoragePool.CompletionGuardStripeCount,
                    volumeRegistry,
                    queueEventJournal,
                    projectionStore,
                    projectionWriteStore);
            });
            services.AddSingleton<IStoragePool>(sp => sp.GetRequiredService<StoragePool>());

            // Register StorageCleanupService as its concrete type so volumes can be registered
            // by StorageVolumeInitializationService. IStorageCleanupService forwards to the same instance.
            services.AddSingleton<StorageCleanupService>(sp =>
            {
                var metadataRepo = sp.GetRequiredService<MetadataRepository>();
                var quotaRepo = sp.GetRequiredService<DirectoryQuotaRepository>();
                var tenantQuotaManager = sp.GetRequiredService<ITenantQuotaManager>();
                var tenantManager = sp.GetRequiredService<ITenantManager>();
                var directoryQuotaManager = sp.GetRequiredService<IDirectoryQuotaManager>();
                var projectionStore = sp.GetRequiredService<IQueueProjectionStore>();
                var projectionCleanupStore = sp.GetRequiredService<IQueueProjectionCleanupStore>();
                var metadataMaintenanceStore = sp.GetRequiredService<IMetadataProjectionMaintenanceStore>();
                var quotaMaintenanceStore = sp.GetRequiredService<IQuotaProjectionMaintenanceStore>();
                var fileSystem = sp.GetRequiredService<IFileSystem>();
                var logger = sp.GetRequiredService<ILogger<StorageCleanupService>>();
                var volumeRegistry = sp.GetRequiredService<StorageVolumeRegistry>();
                var queueEventJournal = sp.GetService<IQueueEventJournal>();

                // Volumes are registered by StorageVolumeInitializationService after async mount.
                return new StorageCleanupService(
                    metadataRepo,
                    quotaRepo,
                    tenantQuotaManager,
                    fileSystem,
                    logger,
                    options.MetadataDirectory,
                    options.QuotaDirectory,
                    options.CleanupOptions,
                    volumeRegistry,
                    tenantManager,
                    queueEventJournal,
                    directoryQuotaManager,
                    projectionStore,
                    projectionCleanupStore,
                    metadataMaintenanceStore,
                    quotaMaintenanceStore);
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
                var queueProjectionMaintenanceService = sp.GetService<IQueueProjectionMaintenanceService>();
                var storageCleanupService = sp.GetService<IStorageCleanupService>();
                var projectionStore = sp.GetRequiredService<IQueueProjectionStore>();
                var metadataMaintenanceStore = sp.GetRequiredService<IMetadataProjectionMaintenanceStore>();
                var quotaMaintenanceStore = sp.GetRequiredService<IQuotaProjectionMaintenanceStore>();

                return new Locus.Storage.Data.DatabaseRecoveryService(
                    metadataRepo,
                    quotaRepo,
                    fileSystem,
                    logger,
                    options.MetadataDirectory,
                    options.QuotaDirectory,
                    options.Volumes.ToDictionary(v => v.MountPath, v => v.VolumeId, StringComparer.OrdinalIgnoreCase),
                    queueProjectionMaintenanceService,
                    storageCleanupService,
                    projectionStore,
                    metadataMaintenanceStore,
                    quotaMaintenanceStore);
            });

            if (options.QueueEventJournal.Enabled)
            {
                services.AddSingleton(options.QueueEventJournal);
                services.AddSingleton<IQueueEventJournal>(sp =>
                {
                    var fileSystem = sp.GetRequiredService<IFileSystem>();
                    var logger = sp.GetRequiredService<ILogger<FileQueueEventJournal>>();
                    return new FileQueueEventJournal(fileSystem, logger, options.QueueEventJournal.QueueDirectory);
                });

                services.AddSingleton<QueueEventProjectionService>(sp =>
                {
                    var journal = sp.GetRequiredService<IQueueEventJournal>();
                    var metadataRepository = sp.GetRequiredService<MetadataRepository>();
                    var tenantQuotaManager = sp.GetRequiredService<ITenantQuotaManager>();
                    var directoryQuotaManager = sp.GetRequiredService<IDirectoryQuotaManager>();
                    var fileSystem = sp.GetRequiredService<IFileSystem>();
                    var logger = sp.GetRequiredService<ILogger<QueueEventProjectionService>>();
                    var storageCleanupService = sp.GetRequiredService<IStorageCleanupService>();
                    var projectionStore = sp.GetRequiredService<IQueueProjectionStore>();

                    return new QueueEventProjectionService(
                        journal,
                        metadataRepository,
                        tenantQuotaManager,
                        directoryQuotaManager,
                        fileSystem,
                        options.QueueEventJournal,
                        logger,
                        storageCleanupService,
                        projectionStore);
                });
                services.AddSingleton<IQueueProjectionMaintenanceService>(sp => sp.GetRequiredService<QueueEventProjectionService>());

                if (options.QueueEventJournal.EnableProjection)
                    services.AddHostedService(sp => sp.GetRequiredService<QueueEventProjectionService>());
            }

            // Register background cleanup service if enabled
            if (options.EnableBackgroundCleanup)
            {
                services.AddSingleton(options.CleanupOptions);
                services.AddHostedService<BackgroundCleanupService>();
            }

            // Register orphan file recovery service if enabled
            if (options.EnableOrphanRecovery)
            {
                services.AddSingleton(options.OrphanRecoveryOptions);
                services.AddHostedService<OrphanFileRecoveryService>();
            }

            // Register quota reconciliation maintenance service if enabled
            if (options.EnableQuotaReconciliation)
            {
                services.AddSingleton(options.QuotaReconciliationOptions);
                services.AddHostedService<QuotaReconciliationService>();
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
                        config.ShardingDepth,
                        writeBufferSize: config.WriteBufferSize,
                        copyBufferSize: config.CopyBufferSize,
                        forceFlushAfterWrite: config.ForceFlushAfterWrite);

                default:
                    throw new NotSupportedException($"Volume type '{config.VolumeType}' is not supported");
            }
        }
    }
}
