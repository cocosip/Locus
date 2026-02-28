using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class MetadataRepositoryTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _repository;
        private readonly string _metadataDir;
        private readonly string _tenantId = "tenant-001";

        public MetadataRepositoryTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-metadata-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);

            var logger = new Mock<ILogger<MetadataRepository>>();
            _repository = new MetadataRepository(_fileSystem, logger.Object, _metadataDir);
        }

        [Fact]
        public async Task GetNextPendingFileAsync_DecrementsPendingCounter()
        {
            var file = CreateMetadata("file-001", FileProcessingStatus.Pending);
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            Assert.Equal(1, GetPendingCount(_repository, _tenantId));

            var allocated = await _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);

            Assert.NotNull(allocated);
            Assert.Equal(FileProcessingStatus.Processing, allocated!.Status);
            Assert.Equal(0, GetPendingCount(_repository, _tenantId));
        }

        [Fact]
        public async Task GetNextPendingBatchAsync_DecrementsPendingCounterForAllocatedFiles()
        {
            await _repository.AddOrUpdateAsync(CreateMetadata("file-001", FileProcessingStatus.Pending), CancellationToken.None);
            await _repository.AddOrUpdateAsync(CreateMetadata("file-002", FileProcessingStatus.Pending), CancellationToken.None);
            await _repository.AddOrUpdateAsync(CreateMetadata("file-003", FileProcessingStatus.Pending), CancellationToken.None);

            Assert.Equal(3, GetPendingCount(_repository, _tenantId));

            var allocated = await _repository.GetNextPendingBatchAsync(_tenantId, 2, CancellationToken.None);

            Assert.Equal(2, allocated.Count());
            Assert.Equal(1, GetPendingCount(_repository, _tenantId));
        }

        [Fact]
        public async Task ResetTimedOutFilesAsync_IncrementsPendingCounter()
        {
            var file = CreateMetadata("file-001", FileProcessingStatus.Processing);
            file.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-10);
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            Assert.Equal(0, GetPendingCount(_repository, _tenantId));

            var resetCount = await _repository.ResetTimedOutFilesAsync(TimeSpan.FromMinutes(1), CancellationToken.None);

            Assert.Equal(1, resetCount);
            Assert.Equal(1, GetPendingCount(_repository, _tenantId));
        }

        public void Dispose()
        {
            _repository.Dispose();

            try
            {
                if (_fileSystem.Directory.Exists(_metadataDir))
                    _fileSystem.Directory.Delete(_metadataDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup failures in tests.
            }
        }

        private FileMetadata CreateMetadata(string fileKey, FileProcessingStatus status)
        {
            return new FileMetadata
            {
                FileKey = fileKey,
                TenantId = _tenantId,
                VolumeId = "vol-001",
                PhysicalPath = $"/test/{fileKey}.dat",
                DirectoryPath = "/",
                FileSize = 1024,
                Status = status,
                CreatedAt = DateTime.UtcNow
            };
        }

        private static int GetPendingCount(MetadataRepository repository, string tenantId)
        {
            var field = typeof(MetadataRepository).GetField("_pendingFileCounts", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);

            var counters = (ConcurrentDictionary<string, int>)field!.GetValue(repository)!;
            return counters.TryGetValue(tenantId, out var count) ? count : 0;
        }
    }
}
