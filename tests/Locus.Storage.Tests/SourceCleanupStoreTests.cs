using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Storage.Tests
{
    public sealed class SourceCleanupStoreTests
    {
        [Fact]
        public async Task UpsertAndReload_PreservesOnlyActiveCleanupState()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");
            var sourcePath = Path.Combine(directory, "incoming", "one.dcm");

            try
            {
                long jobId;
                using (var store = new SourceCleanupStore(databasePath))
                {
                    jobId = await store.UpsertAsync(new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = sourcePath,
                        Fingerprint = "fp:v3:10:20:30:hash",
                        FileKey = "file-1",
                        ImportOperationId = "operation-1",
                        Action = SourceCleanupJobAction.Delete,
                        NextAttemptUtc = DateTime.UtcNow
                    });

                    var active = await store.GetActiveAsync(sourcePath, "fp:v3:10:20:30:hash");
                    Assert.NotNull(active);
                    Assert.Equal(jobId, active!.Id);
                }

                using (var reloaded = new SourceCleanupStore(databasePath))
                {
                    var due = await reloaded.GetDueAsync(DateTime.UtcNow.AddMinutes(1), 10);
                    Assert.Single(due);
                    Assert.Equal("file-1", due[0].FileKey);
                    Assert.Equal("operation-1", due[0].ImportOperationId);

                    await reloaded.RemoveAsync(jobId);
                    Assert.Null(await reloaded.GetActiveAsync(sourcePath, "fp:v3:10:20:30:hash"));
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task OpeningLegacyDatabase_AddsImportOperationIdColumnWithoutLosingJobs()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");
            Directory.CreateDirectory(directory);

            try
            {
                using (var connection = new Microsoft.Data.Sqlite.SqliteConnection(
                    new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder
                    {
                        DataSource = databasePath,
                        Pooling = false
                    }.ToString()))
                {
                    connection.Open();
                    using var command = connection.CreateCommand();
                    command.CommandText = @"
CREATE TABLE source_cleanup_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watcher_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    file_key TEXT NOT NULL,
    action INTEGER NOT NULL,
    move_target_path TEXT,
    failure_directory TEXT,
    max_attempts INTEGER NOT NULL,
    retry_initial_delay_ticks INTEGER NOT NULL,
    retry_max_delay_ticks INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL,
    state INTEGER NOT NULL,
    next_attempt_utc TEXT,
    last_error TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    lease_until_utc TEXT,
    UNIQUE(watcher_id, source_path)
);
INSERT INTO source_cleanup_jobs
(watcher_id, tenant_id, source_path, fingerprint, file_key, action, max_attempts,
 retry_initial_delay_ticks, retry_max_delay_ticks, attempt_count, state,
 created_at_utc, updated_at_utc)
VALUES ('watcher', 'tenant', 'legacy.dcm', 'fp:v3:1:2:3:legacy', '', 1, 5, 0, 0, 0, 5,
        '2026-09-22T00:00:00.0000000Z', '2026-09-22T00:00:00.0000000Z');";
                    command.ExecuteNonQuery();
                }

                using (var store = new SourceCleanupStore(databasePath))
                {
                    var job = await store.GetActiveAsync("legacy.dcm", "ignored");
                    Assert.NotNull(job);
                    Assert.Null(job!.ImportOperationId);

                    job.ImportOperationId = "upgraded-operation";
                    await store.UpdateAsync(job);
                }

                using (var reloaded = new SourceCleanupStore(databasePath))
                {
                    var job = await reloaded.GetActiveAsync("legacy.dcm", "ignored");
                    Assert.Equal("upgraded-operation", job!.ImportOperationId);
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task TryClaim_AllowsOnlyOneActiveClaim()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");

            try
            {
                using (var store = new SourceCleanupStore(databasePath))
                {
                    var id = await store.UpsertAsync(new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = Path.Combine(directory, "one.dcm"),
                        Fingerprint = "fp:v3:1:2:3:hash",
                        FileKey = "file-1",
                        Action = SourceCleanupJobAction.Delete,
                        NextAttemptUtc = DateTime.UtcNow
                    });

                    var now = DateTime.UtcNow;
                    Assert.True(await store.TryClaimAsync(id, now, now.AddMinutes(5), CancellationToken.None));
                    Assert.False(await store.TryClaimAsync(id, now, now.AddMinutes(5), CancellationToken.None));
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task TryReserveAsync_RefusesNewJobWhenActiveCapacityIsFull()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");

            try
            {
                using (var store = new SourceCleanupStore(databasePath))
                {
                    await store.UpsertAsync(new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = Path.Combine(directory, "one.dcm"),
                        Fingerprint = "fp:v3:1:2:3:hash",
                        Action = SourceCleanupJobAction.Delete,
                        State = SourceCleanupJobState.Pending
                    });

                    var reserved = await store.TryReserveAsync(
                        new SourceCleanupJob
                        {
                            WatcherId = "watcher",
                            TenantId = "tenant",
                            SourcePath = Path.Combine(directory, "two.dcm"),
                            Fingerprint = "fp:v3:1:2:3:hash-2",
                            Action = SourceCleanupJobAction.Delete
                        },
                        maxActiveJobs: 1,
                        DateTime.UtcNow.AddMinutes(5));

                    Assert.False(reserved);
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task TryReserveAsync_WhenCapacityIsAvailable_AssignsPersistedJobId()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");

            try
            {
                using (var store = new SourceCleanupStore(databasePath))
                {
                    var job = new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = Path.Combine(directory, "reserved.dcm"),
                        Fingerprint = "fp:v3:1:2:3:reserved",
                        Action = SourceCleanupJobAction.Delete
                    };

                    var reserved = await store.TryReserveAsync(
                        job,
                        maxActiveJobs: 10,
                        DateTime.UtcNow.AddMinutes(5));

                    Assert.True(reserved);
                    Assert.True(job.Id > 0);
                    var persisted = await store.GetActiveAsync(job.SourcePath, job.Fingerprint);
                    Assert.NotNull(persisted);
                    Assert.Equal(job.Id, persisted!.Id);
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task TryReserveAsync_WhenSourceIsAlreadyReserved_ReturnsFalseWithoutReplacingJob()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");

            try
            {
                using (var store = new SourceCleanupStore(databasePath))
                {
                    var sourcePath = Path.Combine(directory, "reserved.dcm");
                    var first = new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = sourcePath,
                        Fingerprint = "fp:v3:1:2:3:first",
                        Action = SourceCleanupJobAction.Delete
                    };
                    var competing = new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = sourcePath,
                        Fingerprint = "fp:v3:1:2:3:competing",
                        Action = SourceCleanupJobAction.Delete
                    };

                    Assert.True(await store.TryReserveAsync(
                        first,
                        maxActiveJobs: 10,
                        DateTime.UtcNow.AddMinutes(5)));

                    var reserved = await store.TryReserveAsync(
                        competing,
                        maxActiveJobs: 10,
                        DateTime.UtcNow.AddMinutes(5));

                    Assert.False(reserved);
                    Assert.Equal(0, competing.Id);
                    var persisted = await store.GetActiveAsync(sourcePath, first.Fingerprint);
                    Assert.NotNull(persisted);
                    Assert.Equal(first.Id, persisted!.Id);
                    Assert.Equal(first.Fingerprint, persisted.Fingerprint);
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task GetDueAsync_IncludesOnlyExpiredImportReservations()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");

            try
            {
                using (var store = new SourceCleanupStore(databasePath))
                {
                    await store.UpsertAsync(new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = Path.Combine(directory, "stale.dcm"),
                        Fingerprint = "fp:v3:1:2:3:stale",
                        Action = SourceCleanupJobAction.Delete,
                        State = SourceCleanupJobState.Importing,
                        UpdatedAtUtc = DateTime.UtcNow.AddMinutes(-10)
                    });

                    var due = await store.GetDueAsync(
                        DateTime.UtcNow,
                        10,
                        DateTime.UtcNow.AddMinutes(-5));

                    Assert.Single(due);
                    Assert.Equal(SourceCleanupJobState.Importing, due[0].State);
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }

        [Fact]
        public async Task PruneTerminalAsync_RemovesExpiredKeepAndFailedRows()
        {
            var directory = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            var databasePath = Path.Combine(directory, "source-cleanup.db");

            try
            {
                using (var store = new SourceCleanupStore(databasePath))
                {
                    await store.UpsertAsync(new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = Path.Combine(directory, "keep.dcm"),
                        Fingerprint = "fp:v3:1:2:3:keep",
                        Action = SourceCleanupJobAction.Keep,
                        UpdatedAtUtc = DateTime.UtcNow.AddDays(-2)
                    });
                    await store.UpsertAsync(new SourceCleanupJob
                    {
                        WatcherId = "watcher",
                        TenantId = "tenant",
                        SourcePath = Path.Combine(directory, "failed.dcm"),
                        Fingerprint = "fp:v3:1:2:3:failed",
                        Action = SourceCleanupJobAction.Delete,
                        State = SourceCleanupJobState.Failed,
                        UpdatedAtUtc = DateTime.UtcNow.AddDays(-2)
                    });

                    var removed = await store.PruneTerminalAsync(DateTime.UtcNow.AddDays(-1), 10);

                    Assert.Equal(2, removed);
                    Assert.Null(await store.GetActiveAsync(Path.Combine(directory, "keep.dcm"), "keep"));
                    Assert.Null(await store.GetActiveAsync(Path.Combine(directory, "failed.dcm"), "failed"));
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, recursive: true);
            }
        }
    }
}
