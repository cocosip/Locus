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
    }
}
