using System;
using System.IO;
using Locus.Core.Abstractions;
using Locus.Core.Models;
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
    }
}
