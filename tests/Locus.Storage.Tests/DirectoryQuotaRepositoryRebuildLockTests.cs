using System.IO.Abstractions;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Storage.Tests;

public sealed class DirectoryQuotaRepositoryRebuildLockTests : IDisposable
{
    private const string TenantId = "rebuild-tenant";
    private static readonly TimeSpan DeadlockTimeout = TimeSpan.FromSeconds(5);

    private readonly IFileSystem _fileSystem = new System.IO.Abstractions.FileSystem();
    private readonly string _directory = Path.Combine(Path.GetTempPath(), $"locus-quota-rebuild-{Guid.NewGuid():N}");

    [Fact]
    public async Task BeginDatabaseRebuildAsync_WhenCurrentFlowAlreadyHoldsTenantLock_DoesNotBlock()
    {
        using var repository = CreateRepository();
        await repository.GetOrCreateAsync(TenantId, "docs");

        using (await repository.BeginDatabaseRebuildAsync(TenantId))
        {
            // Quota re-projection during database recovery re-enters the rebuild for the same
            // tenant from the flow that already holds the lock; it must not wait on itself.
            using var nested = await repository
                .BeginDatabaseRebuildAsync(TenantId)
                .WaitAsync(DeadlockTimeout);

            Assert.Null(nested.BackupPath);
        }

        // The outer handle still owns the lock and must have released it (and the bypass) on dispose.
        await repository
            .SetCurrentCountAsync(TenantId, "docs", 3)
            .WaitAsync(DeadlockTimeout);

        Assert.Equal(3, (await repository.GetAsync(TenantId, "docs"))!.CurrentCount);
    }

    [Fact]
    public async Task BeginDatabaseRebuildAsync_WhenLockAcquisitionIsCancelled_LeavesRepositoryUsable()
    {
        using var repository = CreateRepository();
        await repository.GetOrCreateAsync(TenantId, "docs");

        // The rebuild only produces a backup path when it really ran against the database file.
        var databasePath = Path.Combine(_directory, TenantId, "quotas.db");
        Assert.True(File.Exists(databasePath));

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => repository.BeginDatabaseRebuildAsync(TenantId, cancelled.Token));

        // A cancelled acquisition must neither strand the tenant lock nor leave the flow behind, so
        // the next rebuild still runs against the file instead of returning a non-owning handle.
        using var handle = await repository.BeginDatabaseRebuildAsync(TenantId).WaitAsync(DeadlockTimeout);

        Assert.NotNull(handle.BackupPath);
        Assert.True(File.Exists(handle.BackupPath));
    }

    private DirectoryQuotaRepository CreateRepository()
    {
        return new DirectoryQuotaRepository(
            _fileSystem,
            NullLogger<DirectoryQuotaRepository>.Instance,
            _directory,
            enableBackgroundFlush: false);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory))
                Directory.Delete(_directory, recursive: true);
        }
        catch (IOException)
        {
            // Best-effort cleanup of the temporary quota directory.
        }
    }
}
