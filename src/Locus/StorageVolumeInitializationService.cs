using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.FileSystem;
using Locus.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Locus
{
    /// <summary>
    /// Hosted service that asynchronously mounts storage volumes into the StoragePool at startup.
    /// Runs before the application begins accepting requests, ensuring the pool is fully
    /// initialized before any file operations are attempted.
    /// </summary>
    internal sealed class StorageVolumeInitializationService : IHostedService
    {
        private readonly StoragePool _pool;
        private readonly StorageCleanupService _cleanupService;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<StorageVolumeInitializationService> _logger;
        private readonly IReadOnlyList<VolumeConfiguration> _volumeConfigs;
        private readonly IServiceProvider _serviceProvider;

        public StorageVolumeInitializationService(
            StoragePool pool,
            StorageCleanupService cleanupService,
            IFileSystem fileSystem,
            ILogger<StorageVolumeInitializationService> logger,
            IReadOnlyList<VolumeConfiguration> volumeConfigs,
            IServiceProvider serviceProvider)
        {
            _pool = pool ?? throw new ArgumentNullException(nameof(pool));
            _cleanupService = cleanupService ?? throw new ArgumentNullException(nameof(cleanupService));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumeConfigs = volumeConfigs ?? throw new ArgumentNullException(nameof(volumeConfigs));
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        }

        /// <inheritdoc/>
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Mounting {Count} storage volume(s)...", _volumeConfigs.Count);

            foreach (var config in _volumeConfigs)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Create the volume once and share it between pool and cleanup service.
                var volume = CreateVolume(config);
                try
                {
                    await _pool.AddVolumeAsync(volume, config.InitialDelayMs, config.HealthCheckDelayMs, cancellationToken);
                    _cleanupService.RegisterVolume(volume);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to mount volume {VolumeId} at {MountPath}",
                        config.VolumeId, config.MountPath);
                    throw;
                }
            }

            _logger.LogInformation("All {Count} storage volume(s) mounted successfully.", _volumeConfigs.Count);
        }

        /// <inheritdoc/>
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        private IStorageVolume CreateVolume(VolumeConfiguration config)
        {
            switch (config.VolumeType.ToLowerInvariant())
            {
                case "localfilesystem":
                case "local":
                    var logger = (ILogger<LocalFileSystemVolume>)_serviceProvider.GetService(
                        typeof(ILogger<LocalFileSystemVolume>));
                    return new LocalFileSystemVolume(
                        _fileSystem,
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
