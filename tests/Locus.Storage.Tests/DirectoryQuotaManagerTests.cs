using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Exceptions;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class DirectoryQuotaManagerTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly DirectoryQuotaRepository _repository;
        private readonly Mock<ILogger<DirectoryQuotaManager>> _logger;
        private readonly DirectoryQuotaManager _quotaManager;
        private readonly string _quotaDir;
        private const string TenantId = "test-tenant";

        public DirectoryQuotaManagerTests()
        {
            // Use real file system for tests (LiteDB requires real file system)
            _fileSystem = new System.IO.Abstractions.FileSystem();

            // Setup repositories with unique temporary directories
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-quota-{testId}");
            _fileSystem.Directory.CreateDirectory(_quotaDir);

            var repoLogger = new Mock<ILogger<DirectoryQuotaRepository>>();
            _repository = new DirectoryQuotaRepository(_fileSystem, repoLogger.Object, _quotaDir);

            _logger = new Mock<ILogger<DirectoryQuotaManager>>();
            _quotaManager = new DirectoryQuotaManager(_repository, _logger.Object);
        }

        public void Dispose()
        {
            _repository?.Dispose();

            // Cleanup temporary test directory
            try
            {
                if (_fileSystem.Directory.Exists(_quotaDir))
                    _fileSystem.Directory.Delete(_quotaDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }

        [Fact]
        public async Task CanAddFileAsync_ReturnsTrueWhenNoLimitSet()
        {
            // Arrange
            var directoryPath = "/test/dir";

            // Act
            var canAdd = await _quotaManager.CanAddFileAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            Assert.True(canAdd);
        }

        [Fact]
        public async Task CanAddFileAsync_ReturnsTrueWhenBelowLimit()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,10, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Act
            var canAdd = await _quotaManager.CanAddFileAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            Assert.True(canAdd);
        }

        [Fact]
        public async Task CanAddFileAsync_ReturnsFalseWhenAtLimit()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,2, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Act
            var canAdd = await _quotaManager.CanAddFileAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            Assert.False(canAdd);
        }

        [Fact]
        public async Task IncrementFileCountAsync_IncrementsCount()
        {
            // Arrange
            var directoryPath = "/test/dir";

            // Act
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(2, count);
        }

        [Fact]
        public async Task IncrementFileCountAsync_ThrowsWhenLimitExceeded()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,2, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Act & Assert
            await Assert.ThrowsAsync<DirectoryQuotaExceededException>(() =>
                _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None));
        }

        [Fact]
        public async Task DecrementFileCountAsync_DecrementsCount()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Act
            await _quotaManager.DecrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(2, count);
        }

        [Fact]
        public async Task DecrementFileCountAsync_DoesNotGoBelowZero()
        {
            // Arrange
            var directoryPath = "/test/dir";

            // Act - decrement when count is already 0
            await _quotaManager.DecrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.DecrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(0, count);
        }

        [Fact]
        public async Task SetLimitAsync_SetsLimit()
        {
            // Arrange
            var directoryPath = "/test/dir";

            // Act
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,100, CancellationToken.None);

            // Assert
            var limit = await _quotaManager.GetLimitAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(100, limit);
        }

        [Fact]
        public async Task SetLimitAsync_ZeroLimitDisablesQuota()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,2, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Act - set limit to 0 (disable quota)
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,0, CancellationToken.None);

            // Assert - should be able to add more files
            var canAdd = await _quotaManager.CanAddFileAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.True(canAdd);

            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(3, count);
        }

        [Fact]
        public async Task SetLimitAsync_ThrowsWhenLimitIsNegative()
        {
            // Arrange
            var directoryPath = "/test/dir";

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentException>(() =>
                _quotaManager.SetLimitAsync(TenantId, directoryPath,-1, CancellationToken.None));
        }

        [Fact]
        public async Task GetFileCountAsync_ReturnsZeroForNewDirectory()
        {
            // Arrange
            var directoryPath = "/test/newdir";

            // Act
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            Assert.Equal(0, count);
        }

        [Fact]
        public async Task GetLimitAsync_ReturnsZeroForNewDirectory()
        {
            // Arrange
            var directoryPath = "/test/newdir";

            // Act
            var limit = await _quotaManager.GetLimitAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            Assert.Equal(0, limit);
        }

        [Fact]
        public async Task ConcurrentIncrements_AllSucceedBelowLimit()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,100, CancellationToken.None);

            // Act - 50 concurrent increments
            var tasks = new List<Task>();
            for (int i = 0; i < 50; i++)
            {
                tasks.Add(Task.Run(async () =>
                    await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None)));
            }

            await Task.WhenAll(tasks);

            // Assert
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(50, count);
        }

        [Fact]
        public async Task ConcurrentIncrements_SomeFailWhenLimitReached()
        {
            // Arrange
            var directoryPath = "/test/dir";
            await _quotaManager.SetLimitAsync(TenantId, directoryPath,10, CancellationToken.None);

            // Act - 20 concurrent increments (limit is 10)
            var tasks = new List<Task>();
            var successes = 0;
            var failures = 0;
            var lockObj = new object();

            for (int i = 0; i < 20; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
                        lock (lockObj) { successes++; }
                    }
                    catch (DirectoryQuotaExceededException)
                    {
                        lock (lockObj) { failures++; }
                    }
                }));
            }

            await Task.WhenAll(tasks);

            // Assert
            Assert.Equal(10, successes); // Only 10 should succeed
            Assert.Equal(10, failures); // 10 should fail
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(10, count);
        }

        [Fact]
        public async Task IncrementAndDecrement_MaintainsCorrectCount()
        {
            // Arrange
            var directoryPath = "/test/dir";

            // Act
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.DecrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            await _quotaManager.DecrementFileCountAsync(TenantId, directoryPath, CancellationToken.None);

            // Assert
            var count = await _quotaManager.GetFileCountAsync(TenantId, directoryPath, CancellationToken.None);
            Assert.Equal(2, count);
        }

        [Fact]
        public async Task MultipleDirectories_IndependentQuotas()
        {
            // Arrange
            var dir1 = "/test/dir1";
            var dir2 = "/test/dir2";

            await _quotaManager.SetLimitAsync(TenantId, dir1,5, CancellationToken.None);
            await _quotaManager.SetLimitAsync(TenantId, dir2,10, CancellationToken.None);

            // Act
            await _quotaManager.IncrementFileCountAsync(TenantId, dir1, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, dir1, CancellationToken.None);
            await _quotaManager.IncrementFileCountAsync(TenantId, dir2, CancellationToken.None);

            // Assert
            var count1 = await _quotaManager.GetFileCountAsync(TenantId, dir1, CancellationToken.None);
            var count2 = await _quotaManager.GetFileCountAsync(TenantId, dir2, CancellationToken.None);
            var limit1 = await _quotaManager.GetLimitAsync(TenantId, dir1, CancellationToken.None);
            var limit2 = await _quotaManager.GetLimitAsync(TenantId, dir2, CancellationToken.None);

            Assert.Equal(2, count1);
            Assert.Equal(1, count2);
            Assert.Equal(5, limit1);
            Assert.Equal(10, limit2);
        }
    }
}
