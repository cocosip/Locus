using System;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Locus.Storage.Tests
{
    /// <summary>
    /// Regression tests for the corruption-vs-transient classification fix: transient SQLite
    /// failures must never be treated as corruption, because the corruption path deletes the
    /// tenant database and rebuilds it from the hot cache only.
    /// </summary>
    public class SqliteFailureClassificationTests : IDisposable
    {
        private readonly IFileSystem _fileSystem = new System.IO.Abstractions.FileSystem();
        private readonly string _metadataDir;
        private readonly string _quotaDir;

        public SqliteFailureClassificationTests()
        {
            var id = Guid.NewGuid().ToString("N");
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-classify-meta-{id}");
            _quotaDir = Path.Combine(Path.GetTempPath(), $"locus-test-classify-quota-{id}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);
            _fileSystem.Directory.CreateDirectory(_quotaDir);
        }

        public void Dispose()
        {
            SqliteConnection.ClearAllPools();
            try
            {
                if (_fileSystem.Directory.Exists(_metadataDir))
                    _fileSystem.Directory.Delete(_metadataDir, recursive: true);
                if (_fileSystem.Directory.Exists(_quotaDir))
                    _fileSystem.Directory.Delete(_quotaDir, recursive: true);
            }
            catch
            {
            }
        }

        [Theory]
        [InlineData(11)]
        [InlineData(26)]
        public void IsCorruption_TrueForCorruptionCodes(int errorCode)
        {
            Assert.True(SqliteFailureClassifier.IsCorruption(new SqliteException("boom", errorCode)));
            Assert.False(SqliteFailureClassifier.IsTransient(new SqliteException("boom", errorCode)));
        }

        [Theory]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(10)]
        public void IsTransient_TrueForBusyLockedAndIoError(int errorCode)
        {
            Assert.True(SqliteFailureClassifier.IsTransient(new SqliteException("boom", errorCode)));
            Assert.False(SqliteFailureClassifier.IsCorruption(new SqliteException("boom", errorCode)));
        }

        [Fact]
        public void IsTransient_TrueForIOException_AndNotCorruption()
        {
            var exception = new IOException("device is busy");

            Assert.True(SqliteFailureClassifier.IsTransient(exception));
            Assert.False(SqliteFailureClassifier.IsCorruption(exception));
        }

        [Fact]
        public void IsTransient_FalseForUnrelatedException()
        {
            Assert.False(SqliteFailureClassifier.IsTransient(new InvalidOperationException("nope")));
            Assert.False(SqliteFailureClassifier.IsCorruption(new InvalidOperationException("nope")));
        }

        [Fact]
        public async Task MetadataRepository_TransientOpenFailure_RetriesWithoutRebuilding()
        {
            var repository = new FlakyMetadataRepository(_fileSystem, _metadataDir, transientFailures: 1);
            using (repository)
            {
                await repository.AddOrUpdateDirectAsync(CreateMetadata("transient-open-1", "t1"));

                var stored = await repository.GetByTenantAsync("t1", 0, 10);
                Assert.Contains(stored, metadata => metadata.FileKey == "transient-open-1");
                Assert.Equal(2, repository.OpenAttempts);
            }

            SqliteConnection.ClearAllPools();
            var tenantDirectory = Path.Combine(_metadataDir, "t1");
            Assert.True(_fileSystem.File.Exists(Path.Combine(tenantDirectory, "metadata.db")));
            Assert.Empty(_fileSystem.Directory.GetFiles(tenantDirectory, "*.corrupted.*"));
        }

        [Fact]
        public async Task MetadataRepository_CorruptionOpenFailure_RebuildsDatabase()
        {
            using (var seedRepository = new MetadataRepository(
                _fileSystem,
                NullLogger<MetadataRepository>.Instance,
                _metadataDir,
                enableBackgroundPersistence: false))
            {
                await seedRepository.AddOrUpdateDirectAsync(CreateMetadata("corrupt-open-1", "t2"));
            }

            SqliteConnection.ClearAllPools();

            using (var repository = new FlakyMetadataRepository(_fileSystem, _metadataDir, transientFailures: 0, corruptionFailures: 1))
            {
                await repository.AddOrUpdateDirectAsync(CreateMetadata("corrupt-open-2", "t2"));
                Assert.Equal(2, repository.OpenAttempts);
            }

            SqliteConnection.ClearAllPools();
            var tenantDirectory = Path.Combine(_metadataDir, "t2");
            Assert.NotEmpty(_fileSystem.Directory.GetFiles(tenantDirectory, "*.corrupted.*"));
            Assert.True(_fileSystem.File.Exists(Path.Combine(tenantDirectory, "metadata.db")));
        }

        [Fact]
        public async Task DirectoryQuotaRepository_TransientOpenFailure_RetriesWithoutRebuilding()
        {
            using (var repository = new FlakyQuotaRepository(_fileSystem, _quotaDir, transientFailures: 1))
            {
                await repository.SetCurrentCountAsync("t1", "/docs", 3);

                var quota = await repository.GetAsync("t1", "/docs");
                Assert.NotNull(quota);
                Assert.Equal(3, quota!.CurrentCount);
            }

            SqliteConnection.ClearAllPools();
            var tenantDirectory = Path.Combine(_quotaDir, "t1");
            Assert.True(_fileSystem.File.Exists(Path.Combine(tenantDirectory, "quotas.db")));
            Assert.Empty(_fileSystem.Directory.GetFiles(tenantDirectory, "*.corrupted.*"));
        }

        private static FileMetadata CreateMetadata(string fileKey, string tenantId)
        {
            return new FileMetadata
            {
                FileKey = fileKey,
                TenantId = tenantId,
                VolumeId = "v1",
                PhysicalPath = $"/volumes/v1/{tenantId}/{fileKey}",
                DirectoryPath = "/",
                FileSize = 1,
                CreatedAt = DateTime.UtcNow,
                Status = FileProcessingStatus.Pending
            };
        }

        private sealed class FlakyMetadataRepository : MetadataRepository
        {
            private int _corruptionFailures;
            private int _transientFailures;

            public FlakyMetadataRepository(
                IFileSystem fileSystem,
                string metadataDirectory,
                int transientFailures,
                int corruptionFailures = 0)
                : base(fileSystem, NullLogger<MetadataRepository>.Instance, metadataDirectory, enableBackgroundPersistence: false)
            {
                _transientFailures = transientFailures;
                _corruptionFailures = corruptionFailures;
            }

            public int OpenAttempts;

            protected override SqliteConnection OpenAndInitializeConnection(string dbPath)
            {
                OpenAttempts++;

                if (_transientFailures > 0)
                {
                    _transientFailures--;
                    throw new IOException("injected transient I/O failure");
                }

                if (_corruptionFailures > 0)
                {
                    _corruptionFailures--;
                    throw new SqliteException("injected corruption", 11);
                }

                return base.OpenAndInitializeConnection(dbPath);
            }
        }

        private sealed class FlakyQuotaRepository : DirectoryQuotaRepository
        {
            private int _transientFailures;

            public FlakyQuotaRepository(IFileSystem fileSystem, string quotaDirectory, int transientFailures)
                : base(fileSystem, NullLogger<DirectoryQuotaRepository>.Instance, quotaDirectory, enableBackgroundFlush: false)
            {
                _transientFailures = transientFailures;
            }

            public int OpenAttempts;

            protected override SqliteConnection OpenAndInitializeConnection(string dbPath)
            {
                OpenAttempts++;

                if (_transientFailures > 0)
                {
                    _transientFailures--;
                    throw new SqliteException("injected transient I/O failure", 10);
                }

                return base.OpenAndInitializeConnection(dbPath);
            }
        }
    }
}
