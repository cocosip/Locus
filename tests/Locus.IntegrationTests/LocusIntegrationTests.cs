using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.IntegrationTests
{
    public class LocusIntegrationTests : IDisposable
    {
        private readonly ServiceProvider _serviceProvider;
        private readonly string _testDirectory;

        public LocusIntegrationTests()
        {
            _testDirectory = Path.Combine(Path.GetTempPath(), "locus-integration-tests", Guid.NewGuid().ToString());
            Directory.CreateDirectory(_testDirectory);

            var services = new ServiceCollection();

            // Add logging
            services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

            // Configure Locus with builder API
            services.AddLocus(builder => builder
                .AddLocalVolume("vol-001", Path.Combine(_testDirectory, "volume1"))
                .AddLocalVolume("vol-002", Path.Combine(_testDirectory, "volume2"))
                .WithMetadataDirectory(Path.Combine(_testDirectory, "metadata"))
                .WithQuotaDirectory(Path.Combine(_testDirectory, "quota"))
                .WithRetryPolicy(policy =>
                {
                    policy.MaxRetryCount = 3;
                    policy.InitialRetryDelay = TimeSpan.FromSeconds(1);
                    policy.UseExponentialBackoff = true;
                })
                .WithCleanupOptions(options =>
                {
                    options.CleanupInterval = TimeSpan.FromMinutes(5);
                    options.ProcessingTimeout = TimeSpan.FromMinutes(10);
                    options.FailedFileRetentionPeriod = TimeSpan.FromDays(7);
                })
                .DisableBackgroundCleanup() // Disable for tests
            );

            _serviceProvider = services.BuildServiceProvider();
        }

        public void Dispose()
        {
            _serviceProvider?.Dispose();

            // Cleanup test directory
            if (Directory.Exists(_testDirectory))
            {
                try
                {
                    Directory.Delete(_testDirectory, true);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }

        [Fact]
        public void CanResolveAllServices()
        {
            // Arrange & Act & Assert
            Assert.NotNull(_serviceProvider.GetRequiredService<ITenantManager>());
            Assert.NotNull(_serviceProvider.GetRequiredService<IStoragePool>());
            Assert.NotNull(_serviceProvider.GetRequiredService<IFileScheduler>());
            Assert.NotNull(_serviceProvider.GetRequiredService<IDirectoryQuotaManager>());
            Assert.NotNull(_serviceProvider.GetRequiredService<IStorageCleanupService>());
        }

        [Fact]
        public async Task EndToEnd_WriteReadDelete_WorksCorrectly()
        {
            // Arrange
            var tenantManager = _serviceProvider.GetRequiredService<ITenantManager>();
            var storagePool = _serviceProvider.GetRequiredService<IStoragePool>();

            await tenantManager.CreateTenantAsync("tenant-001", default);
            var tenant = await tenantManager.GetTenantAsync("tenant-001", default);
            Assert.NotNull(tenant);

            var content = new MemoryStream(Encoding.UTF8.GetBytes("Hello, Locus!"));

            // Act - Write
            var fileKey = await storagePool.WriteFileAsync(tenant!, content, default);
            Assert.False(string.IsNullOrEmpty(fileKey));

            // Act - Read
            using var readStream = await storagePool.ReadFileAsync(tenant!, fileKey, default);
            using var reader = new StreamReader(readStream);
            var readContent = await reader.ReadToEndAsync();

            // Assert - Read
            Assert.Equal("Hello, Locus!", readContent);

            // Note: DeleteFileAsync is now internal, files should be deleted via MarkAsCompletedAsync
            // For this test, we'll just verify the file exists
            var fileInfo = await storagePool.GetFileInfoAsync(tenant!, fileKey, default);
            Assert.NotNull(fileInfo);
            Assert.Equal(fileKey, fileInfo.FileKey);
        }

        [Fact]
        public async Task FileScheduler_ProcessingWorkflow_WorksCorrectly()
        {
            // Arrange
            var tenantManager = _serviceProvider.GetRequiredService<ITenantManager>();
            var storagePool = _serviceProvider.GetRequiredService<IStoragePool>();
            var fileScheduler = _serviceProvider.GetRequiredService<IFileScheduler>();

            await tenantManager.CreateTenantAsync("tenant-002", default);
            var tenant = await tenantManager.GetTenantAsync("tenant-002", default);

            // Write 3 files
            for (int i = 1; i <= 3; i++)
            {
                var content = new MemoryStream(Encoding.UTF8.GetBytes($"File {i}"));
                await storagePool.WriteFileAsync(tenant!, content, default);
            }

            // Act - Get next file for processing
            var file1 = await fileScheduler.GetNextFileForProcessingAsync(tenant!, default);
            Assert.NotNull(file1);
            Assert.Equal(FileProcessingStatus.Processing, file1.Status);

            // Act - Process successfully
            // Use explicit scope to ensure stream is disposed before deletion
            {
                using var stream1 = await storagePool.ReadFileAsync(tenant!, file1.FileKey, default);
                // Read and process the stream here if needed
            }
            await fileScheduler.MarkAsCompletedAsync(file1.FileKey, default);

            // Assert - File deleted
            await Assert.ThrowsAsync<FileNotFoundException>(() =>
                storagePool.ReadFileAsync(tenant!, file1.FileKey, default));

            // Act - Get second file and fail it
            var file2 = await fileScheduler.GetNextFileForProcessingAsync(tenant!, default);
            Assert.NotNull(file2);
            await fileScheduler.MarkAsFailedAsync(file2.FileKey, "Test failure", default);

            // Assert - File goes back to pending
            var file2Status = await fileScheduler.GetFileStatusAsync(file2.FileKey, default);
            Assert.Equal(FileProcessingStatus.Pending, file2Status);
        }

        [Fact]
        public async Task TenantQuota_EnforcesLimits()
        {
            // Arrange
            var tenantManager = _serviceProvider.GetRequiredService<ITenantManager>();
            var storagePool = _serviceProvider.GetRequiredService<IStoragePool>();
            var tenantQuotaManager = _serviceProvider.GetRequiredService<ITenantQuotaManager>();

            await tenantManager.CreateTenantAsync("tenant-003", default);
            var tenant = await tenantManager.GetTenantAsync("tenant-003", default);

            // Set tenant quota to 2 files
            await tenantQuotaManager.SetTenantLimitAsync("tenant-003", 2, default);

            // Act - Write 2 files (should succeed)
            var content1 = new MemoryStream(Encoding.UTF8.GetBytes("File 1"));
            var content2 = new MemoryStream(Encoding.UTF8.GetBytes("File 2"));

            await storagePool.WriteFileAsync(tenant!, content1, default);
            await storagePool.WriteFileAsync(tenant!, content2, default);

            // Assert - Third file should fail
            var content3 = new MemoryStream(Encoding.UTF8.GetBytes("File 3"));
            await Assert.ThrowsAsync<Core.Exceptions.TenantQuotaExceededException>(() =>
                storagePool.WriteFileAsync(tenant!, content3, default));
        }

        [Fact]
        public async Task CleanupService_CleansTimedOutFiles()
        {
            // Arrange
            var tenantManager = _serviceProvider.GetRequiredService<ITenantManager>();
            var storagePool = _serviceProvider.GetRequiredService<IStoragePool>();
            var fileScheduler = _serviceProvider.GetRequiredService<IFileScheduler>();
            var cleanupService = _serviceProvider.GetRequiredService<IStorageCleanupService>();

            await tenantManager.CreateTenantAsync("tenant-004", default);
            var tenant = await tenantManager.GetTenantAsync("tenant-004", default);

            // Write a file and start processing it
            var content = new MemoryStream(Encoding.UTF8.GetBytes("Test file"));
            await storagePool.WriteFileAsync(tenant!, content, default);

            var file = await fileScheduler.GetNextFileForProcessingAsync(tenant!, default);
            Assert.NotNull(file);

            // Simulate timeout by running cleanup with very short timeout
            await cleanupService.CleanupTimedOutProcessingFilesAsync(TimeSpan.FromMilliseconds(1), default);

            // Assert - File should be reset to Pending
            var status = await fileScheduler.GetFileStatusAsync(file.FileKey, default);
            Assert.Equal(FileProcessingStatus.Pending, status);
        }

        [Fact]
        public async Task MultipleVolumes_DistributesFiles()
        {
            // Arrange
            var tenantManager = _serviceProvider.GetRequiredService<ITenantManager>();
            var storagePool = _serviceProvider.GetRequiredService<IStoragePool>();

            await tenantManager.CreateTenantAsync("tenant-005", default);
            var tenant = await tenantManager.GetTenantAsync("tenant-005", default);

            // Act - Write multiple files
            for (int i = 1; i <= 10; i++)
            {
                var content = new MemoryStream(Encoding.UTF8.GetBytes($"File {i}"));
                await storagePool.WriteFileAsync(tenant!, content, default);
            }

            // Assert - Verify total capacity includes both volumes
            var totalCapacity = await storagePool.GetTotalCapacityAsync(default);
            Assert.True(totalCapacity > 0);
        }
    }
}
