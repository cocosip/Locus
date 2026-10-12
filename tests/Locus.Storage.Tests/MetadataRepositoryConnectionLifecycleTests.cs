using System.Data;
using System.IO.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Locus.Storage.Tests;

public sealed class MetadataRepositoryConnectionLifecycleTests : IDisposable
{
    private const string TenantId = "lifecycle-tenant";
    private readonly System.IO.Abstractions.FileSystem _fileSystem = new();
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"locus-connection-{Guid.NewGuid():N}");

    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    public async Task GetAsync_WhenInitializationIsTemporarilyLocked_RetriesWithoutRebuilding(int errorCode)
    {
        SeedDatabase();
        using var repository = new FailingInitializationRepository(_fileSystem, _directory, errorCode);

        var error = await Assert.ThrowsAsync<SqliteException>(() => repository.GetAsync(TenantId, "existing"));
        Assert.Equal(errorCode, error.SqliteErrorCode);

        var file = await repository.GetAsync(TenantId, "existing");

        Assert.NotNull(file);
        Assert.Equal("existing", file.FileKey);
        Assert.Equal(2, repository.OpenAttempts);
        Assert.Empty(Directory.GetFiles(_directory, "*.corrupted.*", SearchOption.AllDirectories));
        Assert.Equal(1L, CountPersistedFiles("existing"));
    }

    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    public async Task BackgroundPersistence_WhenInitializationIsTemporarilyLocked_ResumesQueuedWrites(int errorCode)
    {
        SeedDatabase();
        using var repository = new FailingInitializationRepository(_fileSystem, _directory, errorCode, background: true);
        await repository.AddOrUpdateAsync(CreateMetadata("queued"));
        Assert.True(repository.FirstFailure.Wait(TimeSpan.FromSeconds(5)), "Persistence never attempted initialization.");

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (CountPersistedFiles("queued") == 0 && DateTime.UtcNow < deadline)
            await Task.Delay(25);

        Assert.Equal(1L, CountPersistedFiles("queued"));
        Assert.Equal(1L, CountPersistedFiles("existing"));
        Assert.Empty(Directory.GetFiles(_directory, "*.corrupted.*", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task GetAsync_WhenStartupLoadFails_DisposesOpenedConnectionAndRetries()
    {
        using var repository = new FailingStartupLoadRepository(_fileSystem, _directory);

        await Assert.ThrowsAsync<SqliteException>(() => repository.GetAsync(TenantId, "missing"));

        Assert.NotNull(repository.FailedConnection);
        Assert.Equal(ConnectionState.Closed, repository.FailedConnection.State);
        Assert.Null(await repository.GetAsync(TenantId, "missing"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MetadataAccess_WhenOptimizationHasEvictedConnection_WaitsForMaintenance(bool allocate)
    {
        SeedDatabase();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        using var accessStarted = new ManualResetEventSlim();
        var armed = false;
        var existenceChecks = 0;
        var databasePath = Path.Combine(_directory, TenantId, "metadata.db");
        var file = new Mock<IFile>();
        file.Setup(f => f.Exists(It.IsAny<string>())).Returns((string path) =>
        {
            // The second database existence probe occurs after cache eviction,
            // immediately before VACUUM opens its dedicated connection.
            if (Volatile.Read(ref armed) && path == databasePath && Interlocked.Increment(ref existenceChecks) == 2)
            {
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(10)))
                    throw new TimeoutException("Maintenance barrier was not released.");
            }
            return _fileSystem.File.Exists(path);
        });
        var fileSystem = new Mock<IFileSystem>();
        fileSystem.SetupGet(f => f.File).Returns(file.Object);
        fileSystem.SetupGet(f => f.Directory).Returns(_fileSystem.Directory);
        fileSystem.SetupGet(f => f.Path).Returns(_fileSystem.Path);
        fileSystem.SetupGet(f => f.FileInfo).Returns(_fileSystem.FileInfo);
        using var repository = CreateRepository(fileSystem.Object);
        Assert.NotNull(await repository.GetAsync(TenantId, "existing"));
        Volatile.Write(ref armed, true);

        var maintenance = Task.Run(() => repository.OptimizeDatabaseAsync(TenantId));
        Task<FileMetadata?>? access = null;
        try
        {
            Assert.True(entered.Wait(TimeSpan.FromSeconds(5)), "Optimization never reached the eviction barrier.");
            access = Task.Run(async () =>
            {
                accessStarted.Set();
                return allocate
                    ? (await repository.GetNextPendingBatchAsync(TenantId, 1)).SingleOrDefault()
                    : await repository.GetAsync(TenantId, "existing");
            });
            Assert.True(accessStarted.Wait(TimeSpan.FromSeconds(5)));
            Assert.NotSame(access, await Task.WhenAny(access, Task.Delay(200)));

            // The lifecycle boundary is tenant-specific, not a global maintenance lock.
            Assert.Null(await Task.Run(() => repository.GetAsync("another-tenant", "missing")).WaitAsync(TimeSpan.FromSeconds(5)));
        }
        finally
        {
            release.Set();
            await maintenance.WaitAsync(TimeSpan.FromSeconds(5));
            if (access != null)
                await access.WaitAsync(TimeSpan.FromSeconds(5));
        }

        var result = await access!;
        Assert.NotNull(result);
        Assert.Equal(allocate ? FileProcessingStatus.Processing : FileProcessingStatus.Pending, result.Status);
        Assert.Equal(1L, CountPersistedFiles("existing"));
    }

    [Fact]
    public async Task OptimizeDatabaseAsync_WhenInitializationIsInFlight_WaitsBeforeClosingConnection()
    {
        SeedDatabase();
        using var repository = new BlockingInitializationRepository(_fileSystem, _directory);
        using var maintenanceStarted = new ManualResetEventSlim();
        var access = Task.Run(() => repository.GetAsync(TenantId, "existing"));
        Task<(long SizeBefore, long SizeAfter)>? maintenance = null;
        try
        {
            Assert.True(repository.Opened.Wait(TimeSpan.FromSeconds(5)));
            maintenance = Task.Run(() =>
            {
                maintenanceStarted.Set();
                return repository.OptimizeDatabaseAsync(TenantId);
            });
            Assert.True(maintenanceStarted.Wait(TimeSpan.FromSeconds(5)));
            Assert.NotSame(maintenance, await Task.WhenAny(maintenance, Task.Delay(200)));
            Assert.Equal(ConnectionState.Open, repository.InitialConnection!.State);
        }
        finally
        {
            repository.Release.Set();
            await access.WaitAsync(TimeSpan.FromSeconds(5));
            if (maintenance != null)
                await maintenance.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.Equal(ConnectionState.Closed, repository.InitialConnection!.State);
        Assert.NotNull(await repository.GetAsync(TenantId, "existing"));
    }

    private MetadataRepository CreateRepository(IFileSystem? fileSystem = null) =>
        new(fileSystem ?? _fileSystem, NullLogger<MetadataRepository>.Instance, _directory, enableBackgroundPersistence: false);

    private void SeedDatabase()
    {
        using var repository = CreateRepository();
        repository.AddOrUpdateAsync(CreateMetadata("existing")).GetAwaiter().GetResult();
    }

    private FileMetadata CreateMetadata(string fileKey) => new()
    {
        FileKey = fileKey,
        TenantId = TenantId,
        VolumeId = "volume-1",
        PhysicalPath = Path.Combine(_directory, fileKey),
        DirectoryPath = "/",
        FileSize = 42,
        CreatedAt = DateTime.UtcNow,
        Status = FileProcessingStatus.Pending
    };

    private long CountPersistedFiles(string fileKey)
    {
        using var connection = new SqliteConnection($"Data Source={Path.Combine(_directory, TenantId, "metadata.db")};Pooling=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM files WHERE file_key = $key;";
        command.Parameters.AddWithValue("$key", fileKey);
        return (long)command.ExecuteScalar()!;
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
            Directory.Delete(_directory, recursive: true);
    }

    private sealed class FailingInitializationRepository : MetadataRepository
    {
        private readonly int _errorCode;
        private int _openAttempts;
        public int OpenAttempts => Volatile.Read(ref _openAttempts);
        public ManualResetEventSlim FirstFailure { get; } = new();

        public FailingInitializationRepository(IFileSystem fileSystem, string directory, int errorCode, bool background = false)
            : base(fileSystem, NullLogger<MetadataRepository>.Instance, directory,
                enableBackgroundPersistence: background, persistenceIntervalSeconds: 1)
        {
            _errorCode = errorCode;
        }

        protected override SqliteConnection OpenAndInitializeConnection(string dbPath)
        {
            if (Interlocked.Increment(ref _openAttempts) == 1)
            {
                FirstFailure.Set();
                throw new SqliteException("Injected temporary initialization lock.", _errorCode);
            }
            return base.OpenAndInitializeConnection(dbPath);
        }
    }

    private sealed class FailingStartupLoadRepository : MetadataRepository
    {
        public SqliteConnection? FailedConnection { get; private set; }

        public FailingStartupLoadRepository(IFileSystem fileSystem, string directory)
            : base(fileSystem, NullLogger<MetadataRepository>.Instance, directory, enableBackgroundPersistence: false) { }

        protected override SqliteConnection OpenAndInitializeConnection(string dbPath)
        {
            var connection = base.OpenAndInitializeConnection(dbPath);
            if (FailedConnection == null)
            {
                FailedConnection = connection;
                using var command = connection.CreateCommand();
                command.CommandText = "DROP TABLE files;";
                command.ExecuteNonQuery();
            }
            return connection;
        }
    }

    private sealed class BlockingInitializationRepository : MetadataRepository
    {
        public ManualResetEventSlim Opened { get; } = new();
        public ManualResetEventSlim Release { get; } = new();
        public SqliteConnection? InitialConnection { get; private set; }

        public BlockingInitializationRepository(IFileSystem fileSystem, string directory)
            : base(fileSystem, NullLogger<MetadataRepository>.Instance, directory, enableBackgroundPersistence: false) { }

        protected override SqliteConnection OpenAndInitializeConnection(string dbPath)
        {
            var connection = base.OpenAndInitializeConnection(dbPath);
            if (InitialConnection == null)
            {
                InitialConnection = connection;
                Opened.Set();
                if (!Release.Wait(TimeSpan.FromSeconds(10)))
                {
                    connection.Dispose();
                    throw new TimeoutException("Initialization barrier was not released.");
                }
            }
            return connection;
        }
    }
}
