using System;
using System.Collections.Generic;
using System.IO;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Locus.Storage.Tests
{
    public class FileWatcherRegistrationTests
    {
        [Fact]
        public void AddLocus_RegistersPeriodicFileWatcherServicesWithoutStartingThem()
        {
            var services = new ServiceCollection();
            var watchRoot = Path.Combine(Path.GetTempPath(), $"locus-watcher-registration-{Guid.NewGuid():N}");

            services.AddLocus(options =>
            {
                options.Volumes.Add(new VolumeConfiguration
                {
                    VolumeId = "vol-001",
                    MountPath = Path.Combine(watchRoot, "volume"),
                    VolumeType = "LocalFileSystem",
                    InitialDelayMs = 0,
                    HealthCheckDelayMs = 0
                });
                options.FileWatcherConfigurationDirectory = Path.Combine(watchRoot, "watchers");
            });

            Assert.Contains(services, descriptor => descriptor.ServiceType == typeof(IFileWatcherOptionsManager));
            Assert.Contains(
                services,
                descriptor => descriptor.ServiceType == typeof(IHostedService)
                    && descriptor.ImplementationType == typeof(BackgroundFileWatcherService));

            Assert.False(Directory.Exists(watchRoot));
        }

        [Fact]
        public async Task AddLocus_FromConfiguration_BindsWatcherDefaultsWithoutStartingServices()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var watchRoot = Path.Combine(Path.GetTempPath(), $"locus-watcher-configuration-{Guid.NewGuid():N}");
            var configuration = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Locus:Volumes:0:VolumeId"] = "vol-001",
                    ["Locus:Volumes:0:MountPath"] = Path.Combine(watchRoot, "volume"),
                    ["Locus:Volumes:0:VolumeType"] = "LocalFileSystem",
                    ["Locus:Volumes:0:InitialDelayMs"] = "0",
                    ["Locus:Volumes:0:HealthCheckDelayMs"] = "0",
                    ["Locus:FileWatcherConfigurationDirectory"] = Path.Combine(watchRoot, "watchers"),
                    ["Locus:FileWatcherOptions:DefaultPollingInterval"] = "00:00:17",
                    ["Locus:FileWatcherOptions:MaxParallelWatcherScans"] = "2"
                })
                .Build();

            services.AddLocus(configuration);

            Assert.False(Directory.Exists(watchRoot));

            try
            {
                using var provider = services.BuildServiceProvider();
                var manager = provider.GetRequiredService<IFileWatcherOptionsManager>();
                var options = await manager.GetOptionsAsync(CancellationToken.None);

                Assert.Equal(TimeSpan.FromSeconds(17), options.DefaultPollingInterval);
                Assert.Equal(2, options.MaxParallelWatcherScans);
            }
            finally
            {
                if (Directory.Exists(watchRoot))
                    Directory.Delete(watchRoot, recursive: true);
            }
        }

        [Fact]
        public void AddLocus_WhenSourceCleanupDisabled_DoesNotRegisterDurableCleanupStore()
        {
            var services = new ServiceCollection();
            services.AddLocus(options =>
            {
                options.Volumes.Add(new VolumeConfiguration
                {
                    VolumeId = "vol-001",
                    MountPath = Path.Combine(Path.GetTempPath(), $"locus-source-cleanup-disabled-{Guid.NewGuid():N}"),
                    VolumeType = "LocalFileSystem",
                    InitialDelayMs = 0,
                    HealthCheckDelayMs = 0
                });
                options.SourceCleanup.Enabled = false;
            });

            Assert.DoesNotContain(services, descriptor => descriptor.ServiceType == typeof(ISourceCleanupStore));
        }
    }
}
