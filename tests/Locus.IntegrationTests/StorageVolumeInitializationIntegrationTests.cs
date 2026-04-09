using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Locus.FileSystem;
using Locus.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Locus.IntegrationTests
{
    public sealed class StorageVolumeInitializationIntegrationTests
    {
        [Fact]
        public async Task StartupMountedLocalVolume_HonorsConfiguredWriteSettings()
        {
            var root = CreateRoot();
            ServiceProvider? serviceProvider = null;
            try
            {
                serviceProvider = BuildServiceProvider(root, builder => builder
                    .AddVolume(volume =>
                    {
                        volume.VolumeId = "vol-custom";
                        volume.MountPath = Path.Combine(root, "volume-custom");
                        volume.VolumeType = "LocalFileSystem";
                        volume.InitialDelayMs = 0;
                        volume.HealthCheckDelayMs = 0;
                        volume.WriteBufferSize = 256 * 1024;
                        volume.CopyBufferSize = 192 * 1024;
                        volume.ForceFlushAfterWrite = false;
                    }));

                await StartStorageVolumeInitializationAsync(serviceProvider);

                var volumeRegistry = serviceProvider.GetRequiredService<StorageVolumeRegistry>();
                var mountedVolume = Assert.Single(volumeRegistry.GetVolumes());
                var localVolume = Assert.IsType<LocalFileSystemVolume>(mountedVolume);

                Assert.Equal(256 * 1024, GetPrivateField<int>(localVolume, "_writeBufferSize"));
                Assert.Equal(192 * 1024, GetPrivateField<int>(localVolume, "_copyBufferSize"));
                Assert.False(GetPrivateField<bool>(localVolume, "_forceFlushAfterWrite"));
            }
            finally
            {
                serviceProvider?.Dispose();
                DeleteRoot(root);
            }
        }

        [Fact]
        public async Task StartupMountsMultipleVolumesInParallel()
        {
            var root = CreateRoot();
            ServiceProvider? serviceProvider = null;
            try
            {
                serviceProvider = BuildServiceProvider(root, builder =>
                {
                    for (var i = 0; i < 3; i++)
                    {
                        var index = i;
                        builder.AddVolume(volume =>
                        {
                            volume.VolumeId = $"vol-{index + 1:D3}";
                            volume.MountPath = Path.Combine(root, $"volume-{index + 1:D3}");
                            volume.VolumeType = "LocalFileSystem";
                            volume.InitialDelayMs = 300;
                            volume.HealthCheckDelayMs = 0;
                        });
                    }
                });

                var stopwatch = Stopwatch.StartNew();
                await StartStorageVolumeInitializationAsync(serviceProvider);
                stopwatch.Stop();

                Assert.True(
                    stopwatch.Elapsed < TimeSpan.FromMilliseconds(700),
                    $"Expected parallel startup mount to finish well below serial time, actual={stopwatch.Elapsed}.");
            }
            finally
            {
                serviceProvider?.Dispose();
                DeleteRoot(root);
            }
        }

        private static ServiceProvider BuildServiceProvider(string root, Action<LocusBuilder> configure)
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddLocus(builder =>
            {
                builder
                    .WithMetadataDirectory(Path.Combine(root, "metadata"))
                    .WithQuotaDirectory(Path.Combine(root, "quota"))
                    .DisableBackgroundCleanup();
                configure(builder);
            });

            return services.BuildServiceProvider();
        }

        private static async Task StartStorageVolumeInitializationAsync(ServiceProvider serviceProvider)
        {
            var startupService = serviceProvider
                .GetServices<IHostedService>()
                .Single(service => string.Equals(service.GetType().Name, "StorageVolumeInitializationService", StringComparison.Ordinal));

            await startupService.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        private static T GetPrivateField<T>(object instance, string fieldName)
        {
            var field = instance.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.NotNull(field);
            return (T)field!.GetValue(instance)!;
        }

        private static string CreateRoot()
        {
            var root = Path.Combine(Path.GetTempPath(), "locus-storage-init-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(root);
            return root;
        }

        private static void DeleteRoot(string root)
        {
            try
            {
                if (Directory.Exists(root))
                    Directory.Delete(root, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors in tests.
            }
        }
    }
}
