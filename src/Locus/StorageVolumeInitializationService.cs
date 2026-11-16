using System;
using System.Collections.Concurrent;
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
    /// Mounting completes before the application begins accepting requests, ensuring the pool is
    /// fully initialized before any file operations are attempted. Write-path cache warmup is
    /// started after a volume is mounted, tracked, and cancelled on shutdown.
    /// </summary>
    internal sealed class StorageVolumeInitializationService : IHostedService
    {
        private static readonly TimeSpan WarmupShutdownTimeout = TimeSpan.FromSeconds(5);

        private readonly StoragePool _pool;
        private readonly StorageCleanupService _cleanupService;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<StorageVolumeInitializationService> _logger;
        private readonly IReadOnlyList<VolumeConfiguration> _volumeConfigs;
        private readonly IServiceProvider _serviceProvider;
        private readonly LocusStartupCoordinator _startupCoordinator;
        private readonly CancellationTokenSource _warmupCancellation = new();
        private readonly ConcurrentBag<Task> _warmupTasks = new();

        public StorageVolumeInitializationService(
            StoragePool pool,
            StorageCleanupService cleanupService,
            IFileSystem fileSystem,
            ILogger<StorageVolumeInitializationService> logger,
            IReadOnlyList<VolumeConfiguration> volumeConfigs,
            IServiceProvider serviceProvider,
            LocusStartupCoordinator startupCoordinator)
        {
            _pool = pool ?? throw new ArgumentNullException(nameof(pool));
            _cleanupService = cleanupService ?? throw new ArgumentNullException(nameof(cleanupService));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _volumeConfigs = volumeConfigs ?? throw new ArgumentNullException(nameof(volumeConfigs));
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _startupCoordinator = startupCoordinator ?? throw new ArgumentNullException(nameof(startupCoordinator));
        }

        /// <inheritdoc/>
        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            _logger.LogInformation("Mounting {Count} storage volume(s)...", _volumeConfigs.Count);

            var mountTasks = _volumeConfigs
                .Select(config => MountVolumeAsync(config, cancellationToken))
                .ToArray();

            try
            {
                await Task.WhenAll(mountTasks).ConfigureAwait(false);
                _startupCoordinator.MarkVolumesReady();
                _logger.LogInformation("All {Count} storage volume(s) mounted successfully.", _volumeConfigs.Count);
            }
            catch (Exception ex)
            {
                _startupCoordinator.Fail(ex);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            // Warmup walks the volume's directory tree; it must not outlive the host. Cancel it and
            // give it a bounded amount of time before shutdown continues.
            _warmupCancellation.Cancel();

            var warmupTasks = _warmupTasks.ToArray();
            if (warmupTasks.Length > 0)
            {
                await Task.WhenAny(
                        Task.WhenAll(warmupTasks),
                        Task.Delay(WarmupShutdownTimeout, cancellationToken))
                    .ConfigureAwait(false);
            }

            // The source is intentionally not disposed: the hosted-service contract allows a host to
            // start this service again, and a disposed source would throw on the next warmup.
        }

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
                    StartWritePathWarmup(volume, writePathWarmup);
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
            IStorageVolumeWritePathWarmup writePathWarmup)
        {
            // The warmup performs a blocking directory walk, so it is dispatched to the thread pool
            // instead of delaying startup. It is tracked so StopAsync can cancel and observe it.
            var warmupTask = Task.Run(async () =>
            {
                try
                {
                    await writePathWarmup.WarmWritePathCacheAsync(_warmupCancellation.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_warmupCancellation.IsCancellationRequested)
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

            _warmupTasks.Add(warmupTask);
        }
    }
}
