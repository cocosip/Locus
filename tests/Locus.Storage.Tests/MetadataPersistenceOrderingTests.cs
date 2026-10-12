using System.Collections;
using System.IO.Abstractions;
using System.Reflection;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Storage.Tests;

public sealed class MetadataPersistenceOrderingTests : IDisposable
{
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"locus-order-{Guid.NewGuid():N}");
    private const string Tenant = "tenant";

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AcrossBatches_OldCoalescedUpsertCannotOverwriteNewerCommit(bool delete)
    {
        using var repository = CreateRepository();
        var old = Metadata("one");
        var older = Invoke(repository, "CreatePersistenceUpsert", old)!;
        Invoke(repository, "CoalescePersistenceOperation", older, 0, "test");
        var newer = delete
            ? Invoke(repository, "CreatePersistenceDelete", Tenant, old.FileKey)!
            : Invoke(repository, "CreatePersistenceUpsert", Completed(old))!;
        Assert.True((bool)Invoke(repository, "TryWritePersistenceOperation", newer)!);

        Assert.True(Drain(repository));
        Assert.True(Drain(repository));
        Assert.Equal(delete ? null : (long?)FileProcessingStatus.Completed, ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    [Fact]
    public async Task PersistenceRetry_AcrossBatches_DoesNotRevertCompletedToPending()
    {
        using var retried = new ManualResetEventSlim();
        using var resume = new ManualResetEventSlim();
        var logger = new Moq.Mock<Microsoft.Extensions.Logging.ILogger<MetadataRepository>>();
        logger.Setup(l => l.Log(Moq.It.IsAny<Microsoft.Extensions.Logging.LogLevel>(), Moq.It.IsAny<Microsoft.Extensions.Logging.EventId>(),
                Moq.It.Is<Moq.It.IsAnyType>((state, type) => state.ToString()!.StartsWith("Requeued failed SQLite batch")),
                Moq.It.IsAny<Exception?>(), Moq.It.IsAny<Func<Moq.It.IsAnyType, Exception?, string>>()))
            .Callback(() =>
            {
                retried.Set();
                if (!resume.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
            });
        using var repository = new BlockingFailureRepository(_directory, logger.Object);
        var old = Metadata("one");
        var older = Invoke(repository, "CreatePersistenceUpsert", old)!;
        Invoke(repository, "TryWritePersistenceOperation", older);

        try
        {
            Assert.True(repository.Entered.Wait(TimeSpan.FromSeconds(5)));
            var newer = Invoke(repository, "CreatePersistenceUpsert", Completed(old))!;
            Assert.True((bool)Invoke(repository, "TryWritePersistenceOperation", newer)!);
        }
        finally
        {
            repository.Release.Set();
        }
        Assert.True(retried.Wait(TimeSpan.FromSeconds(5)), "Old operation was never requeued after SQLITE_BUSY.");
        var disposal = Task.Run(repository.Dispose);
        try
        {
            var cts = (CancellationTokenSource)typeof(MetadataRepository)
                .GetField("_persistenceCts", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(repository)!;
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!cts.IsCancellationRequested && DateTime.UtcNow < deadline) await Task.Delay(10);
            Assert.True(cts.IsCancellationRequested);
        }
        finally { resume.Set(); }
        await disposal.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal((long)FileProcessingStatus.Completed, ReadStatus(old.FileKey));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DirectProjectionCommit_SupersedesOlderQueuedUpsert(bool delete)
    {
        using var repository = CreateRepository();
        var old = Metadata("one");
        await repository.AddOrUpdateAsync(old);
        var older = Invoke(repository, "CreatePersistenceUpsert", old)!;
        Invoke(repository, "TryWritePersistenceOperation", older);
        var store = new MetadataRepositoryQueueProjectionStore(repository);
        if (delete)
            Assert.True(await store.RemoveProjectedFileAsync(Tenant, old.FileKey));
        else
            await store.UpsertProjectedFileAsync(Completed(old));

        Assert.True(Drain(repository));
        Assert.Equal(delete ? null : (long?)FileProcessingStatus.Completed, ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    private MetadataRepository CreateRepository() => new(new System.IO.Abstractions.FileSystem(),
        NullLogger<MetadataRepository>.Instance, _directory, enableBackgroundPersistence: false,
        maxDrainBatchSize: 1, maxPersistenceQueueSize: 1);

    [Fact]
    public async Task ColdRemove_SupersedesOlderPendingUpsert()
    {
        using var repository = CreateRepository();
        var old = Metadata("cold");
        await repository.AddOrUpdateAsync(Completed(old));
        Invoke(repository, "TryWritePersistenceOperation", Invoke(repository, "CreatePersistenceUpsert", old)!);
        Assert.True(await repository.RemoveAsync(Tenant, old.FileKey));
        Assert.True(Drain(repository));
        Assert.Null(ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    [Fact]
    public async Task CancelledProjectionBatch_RollsBackAndReleasesOutstandingOperations()
    {
        using var repository = CreateRepository();
        var old = Metadata("cancelled");
        await repository.AddOrUpdateAsync(old);
        var batch = repository.BeginProjectionBatch(Tenant);
        await batch.UpsertProjectedFileAsync(Completed(old));
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => batch.FlushAsync(cancellation.Token));
        Assert.Equal(FileProcessingStatus.Pending, (await repository.GetAsync(Tenant, old.FileKey))!.Status);
        AssertTrackingReleased(repository);
    }

    [Fact]
    public async Task FailedProjectionBatch_RollsBackAndAllowsOlderUncommittedStateToPersist()
    {
        using var repository = CreateRepository();
        var old = Metadata("failed");
        await repository.AddOrUpdateAsync(old);
        Invoke(repository, "TryWritePersistenceOperation", Invoke(repository, "CreatePersistenceUpsert", old)!);
        ExecuteSql("CREATE TRIGGER fail_write BEFORE INSERT ON files BEGIN SELECT RAISE(FAIL, 'injected failure'); END;");
        var batch = repository.BeginProjectionBatch(Tenant);
        await batch.UpsertProjectedFileAsync(Completed(old));
        await Assert.ThrowsAsync<SqliteException>(() => batch.FlushAsync());
        Assert.Equal(FileProcessingStatus.Pending, (await repository.GetAsync(Tenant, old.FileKey))!.Status);
        ExecuteSql("DROP TRIGGER fail_write;");
        Assert.True(Drain(repository));
        Assert.Equal((long)FileProcessingStatus.Pending, ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    [Fact]
    public async Task AbandonedProjectionBatch_RollsBackAndReleasesTracking()
    {
        using var repository = CreateRepository();
        var old = Metadata("abandoned");
        await repository.AddOrUpdateAsync(old);
        var batch = repository.BeginProjectionBatch(Tenant);
        await batch.UpsertProjectedFileAsync(Completed(old));
        ((IDisposable)batch).Dispose();
        Assert.Equal(FileProcessingStatus.Pending, (await repository.GetAsync(Tenant, old.FileKey))!.Status);
        AssertTrackingReleased(repository);
        var empty = repository.BeginProjectionBatch(Tenant);
        Assert.False(await empty.RemoveProjectedFileAsync("absent"));
        ((IDisposable)empty).Dispose();
    }

    [Theory]
    [InlineData(FileProcessingStatus.Processing)]
    [InlineData(FileProcessingStatus.Completed)]
    public async Task AtomicRecovery_ExistingSqliteRecordOutsideHotCacheIsPreserved(FileProcessingStatus status)
    {
        using var repository = CreateRepository();
        Assert.Null(await repository.GetAsync(Tenant, "external"));
        var existing = Metadata("external");
        existing.Status = status;
        using (var other = CreateRepository())
            await other.AddOrUpdateAsync(existing);
        var recovery = new MetadataRepositoryQueueProjectionStore(repository);
        Assert.False(await recovery.TryAddRecoveredFileAsync(Metadata("external")));
        Assert.Equal((long)status, ReadStatus(existing.FileKey));
    }

    [Fact]
    public void CoalescingAndMultipleKeys_ReleaseTrackingAfterAllBatchesDrain()
    {
        using var repository = CreateRepository();
        for (var index = 0; index < 50; index++)
        {
            var old = Metadata($"file-{index}");
            Invoke(repository, "CoalescePersistenceOperation", Invoke(repository, "CreatePersistenceUpsert", old)!, 0, "test");
            Invoke(repository, "CoalescePersistenceOperation", Invoke(repository, "CreatePersistenceUpsert", Completed(old))!, 0, "test");
        }
        while (Drain(repository)) { }
        for (var index = 0; index < 50; index++)
            Assert.Equal((long)FileProcessingStatus.Completed, ReadStatus($"file-{index}"));
        AssertTrackingReleased(repository);
    }

    private void ExecuteSql(string sql)
    {
        using var connection = new SqliteConnection($"Data Source={Path.Combine(_directory, Tenant, "metadata.db")};Pooling=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConditionalColdDelete_SupersedesOlderQueuedUpsert(bool permanentlyFailed)
    {
        using var repository = CreateRepository();
        var old = Metadata("conditional-delete");
        var current = Completed(old);
        var timestamp = DateTime.UtcNow;
        current.Status = permanentlyFailed ? FileProcessingStatus.PermanentlyFailed : FileProcessingStatus.Completed;
        current.CompletedAt = current.LastFailedAt = timestamp;
        await repository.AddOrUpdateAsync(current);
        Invoke(repository, "TryWritePersistenceOperation", Invoke(repository, "CreatePersistenceUpsert", old)!);
        Assert.True(permanentlyFailed
            ? await repository.TryRemovePermanentlyFailedFileAsync(Tenant, current.FileKey, timestamp)
            : await repository.TryRemoveCompletedFileAsync(Tenant, current.FileKey, timestamp));
        Assert.True(Drain(repository));
        Assert.Null(ReadStatus(current.FileKey));
        AssertTrackingReleased(repository);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AllocationRegistration_WhenNewerCompletionInterleaves_DoesNotPersistOldProcessing(bool batch)
    {
        using var repository = CreateRepository();
        var old = Metadata("allocated");
        await repository.AddOrUpdateAsync(old);
        var locks = (System.Collections.Concurrent.ConcurrentDictionary<string, object>)typeof(MetadataRepository)
            .GetField("_processingByStartTimeLocks", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(repository)!;
        var indexLock = locks.GetOrAdd(Tenant, _ => new object());
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var barrier = Task.Run(() =>
        {
            lock (indexLock)
            {
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
            }
        });
        Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
        var allocation = Task.Run(async () =>
        {
            if (batch) await repository.GetNextPendingBatchAsync(Tenant, 1);
            else await repository.GetNextPendingFileAsync(Tenant);
        });
        Task? completion = null;
        try
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while ((await repository.GetAsync(Tenant, old.FileKey))!.Status != FileProcessingStatus.Processing
                && DateTime.UtcNow < deadline) await Task.Delay(10);
            Assert.Equal(FileProcessingStatus.Processing, (await repository.GetAsync(Tenant, old.FileKey))!.Status);
            completion = Task.Run(() => new MetadataRepositoryQueueProjectionStore(repository).UpsertProjectedFileAsync(Completed(old)));
            await Task.WhenAny(completion, Task.Delay(200));
        }
        finally { release.Set(); }
        await Task.WhenAll(barrier, allocation, completion!).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal((long)FileProcessingStatus.Completed, ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    [Fact]
    public async Task ProjectionRollback_CannotOverwriteConcurrentProcessingUpdate()
    {
        using var repository = CreateRepository();
        var old = Metadata("rollback-race");
        await repository.AddOrUpdateAsync(old);
        var staged = old.Clone();
        staged.Status = FileProcessingStatus.Processing;
        staged.ProcessingStartTime = DateTime.UtcNow;
        var batch = repository.BeginProjectionBatch(Tenant);
        await batch.UpsertProjectedFileAsync(staged);
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var update = Task.Run(() => repository.TryUpdateProcessingFileAsync(Tenant, old.FileKey,
            staged.ProcessingStartTime.Value, file =>
            {
                entered.Set();
                if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
                return Completed(file);
            }));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
        var rollback = Task.Run(() => ((IDisposable)batch).Dispose());
        try { await Task.WhenAny(rollback, Task.Delay(200)); }
        finally { release.Set(); }
        await Task.WhenAll(update, rollback).WaitAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(await update);
        Assert.Equal(FileProcessingStatus.Completed, (await repository.GetAsync(Tenant, old.FileKey))!.Status);
        Assert.Equal((long)FileProcessingStatus.Completed, ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    [Fact]
    public async Task DiagnosticFailureAfterCommit_DoesNotReplaySettledOperation()
    {
        var logger = new Moq.Mock<Microsoft.Extensions.Logging.ILogger<MetadataRepository>>();
        var failures = 0;
        logger.Setup(l => l.Log(Moq.It.IsAny<Microsoft.Extensions.Logging.LogLevel>(), Moq.It.IsAny<Microsoft.Extensions.Logging.EventId>(),
                Moq.It.Is<Moq.It.IsAnyType>((state, type) => state.ToString()!.StartsWith("Flushed 1 persistence operations")),
                Moq.It.IsAny<Exception?>(), Moq.It.IsAny<Func<Moq.It.IsAnyType, Exception?, string>>()))
            .Callback(() => { if (Interlocked.Increment(ref failures) == 1) throw new InvalidOperationException("Injected diagnostic failure"); });
        using var repository = new MetadataRepository(new System.IO.Abstractions.FileSystem(), logger.Object,
            _directory, enableBackgroundPersistence: false, maxDrainBatchSize: 1);
        var old = Metadata("diagnostic");
        var operation = Invoke(repository, "CreatePersistenceUpsert", old)!;
        // Drive the real retry path manually so no background drain races this assertion.
        var enabled = typeof(MetadataRepository).GetField("_enableBackgroundPersistence", BindingFlags.Instance | BindingFlags.NonPublic)!;
        enabled.SetValue(repository, true);
        Invoke(repository, "TryWritePersistenceOperation", operation);
        Assert.False(Drain(repository));
        enabled.SetValue(repository, false);
        await repository.AddOrUpdateAsync(Completed(old));
        while (Drain(repository)) { }
        Assert.Equal((long)FileProcessingStatus.Completed, ReadStatus(old.FileKey));
        AssertTrackingReleased(repository);
    }

    private FileMetadata Metadata(string key) => new()
    {
        TenantId = Tenant, FileKey = key, VolumeId = "volume", PhysicalPath = Path.Combine(_directory, key),
        DirectoryPath = "/", CreatedAt = DateTime.UtcNow, Status = FileProcessingStatus.Pending
    };

    private static FileMetadata Completed(FileMetadata file)
    {
        var result = file.Clone();
        result.Status = FileProcessingStatus.Completed;
        result.CompletedAt = DateTime.UtcNow;
        return result;
    }

    private long? ReadStatus(string key)
    {
        using var connection = new SqliteConnection($"Data Source={Path.Combine(_directory, Tenant, "metadata.db")};Pooling=False");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT status FROM files WHERE file_key = $key";
        command.Parameters.AddWithValue("$key", key);
        return command.ExecuteScalar() as long?;
    }

    private static bool Drain(MetadataRepository repository) => (bool)Invoke(repository, "DrainPersistenceBatch")!;
    private static void AssertTrackingReleased(MetadataRepository repository)
    {
        var tracking = (IDictionary)typeof(MetadataRepository)
            .GetField("_persistenceOrder", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(repository)!;
        Assert.Empty(tracking);
    }
    private static object? Invoke(MetadataRepository repository, string name, params object[] args) =>
        typeof(MetadataRepository).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(repository, args);

    public void Dispose()
    {
        if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
    }

    private sealed class BlockingFailureRepository : MetadataRepository
    {
        public ManualResetEventSlim Entered { get; } = new();
        public ManualResetEventSlim Release { get; } = new();
        private int _attempts;
        public BlockingFailureRepository(string directory, Microsoft.Extensions.Logging.ILogger<MetadataRepository> logger) : base(new System.IO.Abstractions.FileSystem(),
            logger, directory, enableBackgroundPersistence: true,
            maxDrainBatchSize: 1, maxPersistenceQueueSize: 1) { }

        protected override SqliteConnection OpenAndInitializeConnection(string dbPath)
        {
            if (Interlocked.Increment(ref _attempts) == 1)
            {
                Entered.Set();
                if (!Release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException();
                throw new SqliteException("Injected SQLITE_BUSY", 5);
            }
            return base.OpenAndInitializeConnection(dbPath);
        }
    }
}
