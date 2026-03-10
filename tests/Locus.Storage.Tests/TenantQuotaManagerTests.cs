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
                _quotaDir);
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
