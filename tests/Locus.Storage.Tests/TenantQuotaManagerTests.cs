using System;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Exceptions;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class TenantQuotaManagerTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly DirectoryQuotaRepository _repository;
        private readonly TenantQuotaManager _manager;
        private readonly string _quotaDir;

        public TenantQuotaManagerTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-tenant-quota-{testId}");
            _fileSystem.Directory.CreateDirectory(_quotaDir);

            _repository = new DirectoryQuotaRepository(
                _fileSystem,
                new Mock<ILogger<DirectoryQuotaRepository>>().Object,
                _quotaDir,
                enableBackgroundFlush: false);
            _manager = new TenantQuotaManager(
                _repository,
                new Mock<ILogger<TenantQuotaManager>>().Object);
        }

        [Fact]
        public async Task GlobalLimitReduction_AppliesToExistingTenantWithoutExplicitOverride()
        {
            const string tenantId = "tenant-global-reduced";

            await _manager.SetGlobalLimitAsync(2, CancellationToken.None);
            await _manager.IncrementFileCountAsync(tenantId, CancellationToken.None);

            await _manager.SetGlobalLimitAsync(1, CancellationToken.None);

            await Assert.ThrowsAsync<TenantQuotaExceededException>(() =>
                _manager.IncrementFileCountAsync(tenantId, CancellationToken.None));
        }

        [Fact]
        public async Task GlobalLimitIncrease_AppliesToExistingTenantWithoutExplicitOverride()
        {
            const string tenantId = "tenant-global-increased";

            await _manager.SetGlobalLimitAsync(1, CancellationToken.None);
            await _manager.IncrementFileCountAsync(tenantId, CancellationToken.None);

            await _manager.SetGlobalLimitAsync(2, CancellationToken.None);
            await _manager.IncrementFileCountAsync(tenantId, CancellationToken.None);

            var count = await _manager.GetFileCountAsync(tenantId, CancellationToken.None);
            Assert.Equal(2, count);
        }

        [Fact]
        public async Task SetGlobalLimitAsync_ClearsReadOnlyDatabaseAttributeAndPersistsUpdate()
        {
            await _manager.SetGlobalLimitAsync(1, CancellationToken.None);

            _repository.Dispose();
            SqliteConnection.ClearAllPools();

            var tenantDir = Path.Combine(_quotaDir, "_GLOBAL_QUOTA_");
            var dbPath = Path.Combine(tenantDir, "quotas.db");
            Assert.True(File.Exists(dbPath));

            File.SetAttributes(dbPath, File.GetAttributes(dbPath) | FileAttributes.ReadOnly);

            using var repository = new DirectoryQuotaRepository(
                _fileSystem,
                new Mock<ILogger<DirectoryQuotaRepository>>().Object,
                _quotaDir,
                enableBackgroundFlush: false);
            var manager = new TenantQuotaManager(
                repository,
                new Mock<ILogger<TenantQuotaManager>>().Object);

            await manager.SetGlobalLimitAsync(2, CancellationToken.None);

            Assert.False(new System.IO.FileInfo(dbPath).IsReadOnly);
            Assert.Equal(2, await manager.GetGlobalLimitAsync(CancellationToken.None));
        }

        [Fact]
        public async Task RemoveTenantLimitAsync_DisablesTenantSpecificQuotaRecord()
        {
            const string tenantId = "tenant-limit-disabled";

            await _manager.SetTenantLimitAsync(tenantId, 5, CancellationToken.None);
            await _manager.RemoveTenantLimitAsync(tenantId, CancellationToken.None);

            var quota = await _repository.GetAsync(tenantId, tenantId, CancellationToken.None);
            Assert.NotNull(quota);
            Assert.Equal(0, quota!.MaxCount);
            Assert.False(quota.Enabled);
        }

        [Fact]
        public async Task SetGlobalLimitAsync_WithZero_DisablesGlobalQuotaRecord()
        {
            await _manager.SetGlobalLimitAsync(0, CancellationToken.None);

            var quota = await _repository.GetAsync("_GLOBAL_QUOTA_", "_GLOBAL_QUOTA_", CancellationToken.None);
            Assert.NotNull(quota);
            Assert.Equal(0, quota!.MaxCount);
            Assert.False(quota.Enabled);
        }

        [Fact]
        public async Task ApplyAcceptedProjectionAsync_ConsumesReservationAndKeepsTotalStable()
        {
            const string tenantId = "tenant-projection-accepted";

            await _manager.SetTenantLimitAsync(tenantId, 2, CancellationToken.None);
            await _manager.IncrementFileCountAsync(tenantId, CancellationToken.None);

            var consumedReservation = await _manager.ApplyAcceptedProjectionAsync(tenantId, CancellationToken.None);
            var count = await _manager.GetFileCountAsync(tenantId, CancellationToken.None);

            Assert.True(consumedReservation);
            Assert.Equal(1, count);
            Assert.Equal(1, (await _repository.GetAsync(tenantId, tenantId, CancellationToken.None))!.CurrentCount);
        }

        [Fact]
        public async Task ApplyDeleteSucceededProjectionAsync_DecrementsProjectedCountWithoutTouchingReservations()
        {
            const string tenantId = "tenant-projection-delete";

            await _manager.ApplyAcceptedProjectionAsync(tenantId, CancellationToken.None);
            await _manager.IncrementFileCountAsync(tenantId, CancellationToken.None);

            await _manager.ApplyDeleteSucceededProjectionAsync(tenantId, CancellationToken.None);

            var count = await _manager.GetFileCountAsync(tenantId, CancellationToken.None);
            Assert.Equal(1, count);
            Assert.Equal(0, (await _repository.GetAsync(tenantId, tenantId, CancellationToken.None))!.CurrentCount);
        }

        [Fact]
        public async Task SetGlobalLimitAsync_UsesQuotaProjectionStoreWhenProvided()
        {
            var existing = new DirectoryQuota
            {
                DirectoryPath = "_GLOBAL_QUOTA_",
                CurrentCount = 0,
                MaxCount = 1,
                Enabled = true,
                CreatedAt = DateTime.UtcNow.AddMinutes(-1),
                LastUpdated = DateTime.UtcNow.AddMinutes(-1)
            };

            var quotaStore = new Mock<IQuotaProjectionStore>(MockBehavior.Strict);
            quotaStore
                .Setup(store => store.GetOrCreateQuotaAsync("_GLOBAL_QUOTA_", "_GLOBAL_QUOTA_", It.IsAny<CancellationToken>()))
                .ReturnsAsync(existing);
            quotaStore
                .Setup(store => store.UpdateQuotaAsync(
                    "_GLOBAL_QUOTA_",
                    It.Is<DirectoryQuota>(quota => quota.DirectoryPath == "_GLOBAL_QUOTA_" && quota.MaxCount == 7 && quota.Enabled),
                    It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);

            var manager = new TenantQuotaManager(quotaStore.Object, new Mock<ILogger<TenantQuotaManager>>().Object);

            await manager.SetGlobalLimitAsync(7, CancellationToken.None);

            quotaStore.VerifyAll();
        }

        public void Dispose()
        {
            _repository.Dispose();
            SqliteConnection.ClearAllPools();

            try
            {
                if (_fileSystem.Directory.Exists(_quotaDir))
                    _fileSystem.Directory.Delete(_quotaDir, recursive: true);
            }
            catch
            {
            }
        }
    }
}
