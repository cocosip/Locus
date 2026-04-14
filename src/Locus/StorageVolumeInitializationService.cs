using System;
using System.Collections.Generic;
using System.Linq;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
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
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Mounting {Count} storage volume(s)...", _volumeConfigs.Count);

            var mountTasks = _volumeConfigs
                .Select(config => MountVolumeAsync(config, cancellationToken))
                .ToArray();

            await Task.WhenAll(mountTasks).ConfigureAwait(false);

            _logger.LogInformation("All {Count} storage volume(s) mounted successfully.", _volumeConfigs.Count);
        }

        /// <inheritdoc/>
        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        private async Task MountVolumeAsync(VolumeConfiguration config, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var volume = StorageVolumeFactory.CreateVolume(config, _fileSystem, _serviceProvider);
            try
            {
                await _pool.AddVolumeAsync(volume, config.InitialDelayMs, config.HealthCheckDelayMs, cancellationToken)
                    .ConfigureAwait(false);

                _cleanupService.RegisterVolume(volume);

                if (volume is IStorageVolumeWritePathWarmup writePathWarmup)
                    StartWritePathWarmup(volume, writePathWarmup, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to mount volume {VolumeId} at {MountPath}",
                    config.VolumeId,
                    config.MountPath);
                throw;
            }
        }

        private void StartWritePathWarmup(
            IStorageVolume volume,
            IStorageVolumeWritePathWarmup writePathWarmup,
            CancellationToken cancellationToken)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await writePathWarmup.WarmWritePathCacheAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Write-path cache warmup failed for volume {VolumeId} at {MountPath}",
                        volume.VolumeId,
                        volume.MountPath);
                }
            }, CancellationToken.None);
        }
    }
}
