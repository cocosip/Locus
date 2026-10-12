using System;
using System.IO;
using System.IO.Abstractions;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Moq;
using Microsoft.Extensions.Logging.Abstractions;

namespace Locus.Storage.Tests
{
    public sealed class SourceCleanupWorkerTests
    {
        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public async Task ProcessDueJobs_WhenSourceReadIsLocked_RetainsJobForRetry(bool movePending, bool exhausted)
        {
            var directory = CreateDirectory();
            try
            {
                var source = Path.Combine(directory, "locked.dcm");
                File.WriteAllText(source, "content");
                var job = CreateJob(directory, source, FingerprintFor(source));
                job.ImportOperationId = "same-import";
                job.RetryInitialDelay = job.RetryMaxDelay = TimeSpan.FromMinutes(1);
                job.FailureDirectory = Path.Combine(directory, "failed");
                job.MaxAttempts = exhausted ? 1 : 3;
                job.State = movePending ? SourceCleanupJobState.MovePending : SourceCleanupJobState.Pending;
                using var store = new SourceCleanupStore(Path.Combine(directory, "state.db"));
                var id = await store.UpsertAsync(job);
                var worker = CreateWorker(store);
                using (File.Open(source, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                var retained = await store.GetActiveAsync(source, job.Fingerprint);
                Assert.NotNull(retained);
                Assert.Equal(id, retained!.Id);
                Assert.Equal("same-import", retained.ImportOperationId);
                Assert.Equal(1, retained.AttemptCount);
                Assert.Equal(movePending || exhausted ? SourceCleanupJobState.MovePending : SourceCleanupJobState.Retrying, retained.State);
                Assert.NotNull(retained.NextAttemptUtc);
                Assert.False(string.IsNullOrWhiteSpace(retained.LastError));
                retained.NextAttemptUtc = DateTime.UtcNow.AddSeconds(-1);
                await store.UpdateAsync(retained);
                await worker.ProcessDueJobsAsync(CancellationToken.None);
                Assert.False(File.Exists(source));
                Assert.Null(await store.GetActiveAsync(source, job.Fingerprint));
                if (movePending || exhausted)
                    Assert.True(File.Exists(Path.Combine(job.FailureDirectory, "watcher", "locked.dcm")));
            }
            finally { DeleteDirectory(directory); }
        }

        [Fact]
        public async Task ProcessDueJobs_WhenFingerprintReadIsUnauthorized_RetriesInsteadOfCompleting()
        {
            var directory = CreateDirectory();
            try
            {
                var source = Path.Combine(directory, "permission.dcm");
                File.WriteAllText(source, "content");
                var job = CreateJob(directory, source, FingerprintFor(source));
                job.RetryInitialDelay = job.RetryMaxDelay = TimeSpan.FromMinutes(1);
                using var store = new SourceCleanupStore(Path.Combine(directory, "state.db"));
                await store.UpsertAsync(job);
                var file = new Mock<IFile>();
                file.Setup(f => f.Exists(source)).Returns(true);
                file.Setup(f => f.OpenRead(source)).Throws(new UnauthorizedAccessException("Injected permission failure"));
                var fs = new Mock<IFileSystem>();
                fs.SetupGet(f => f.File).Returns(file.Object);
                fs.SetupGet(f => f.FileInfo).Returns(new System.IO.Abstractions.FileSystem().FileInfo);
                var worker = new SourceCleanupWorker(store, fs.Object,
                    new SourceCleanupOptions { Enabled = true }, NullLogger<SourceCleanupWorker>.Instance);

                await worker.ProcessDueJobsAsync(CancellationToken.None);

                var retained = await store.GetActiveAsync(source, job.Fingerprint);
                Assert.Equal(SourceCleanupJobState.Retrying, retained!.State);
                Assert.Equal(1, retained.AttemptCount);
                file.Verify(f => f.Delete(It.IsAny<string>()), Times.Never);
            }
            finally { DeleteDirectory(directory); }
        }

        [Fact]
        public async Task ProcessDueJobs_WhenCancelledDuringFingerprintRead_DoesNotRecordFailure()
        {
            var directory = CreateDirectory();
            try
            {
                var source = Path.Combine(directory, "cancelled.dcm");
                File.WriteAllText(source, "content");
                var job = CreateJob(directory, source, FingerprintFor(source));
                using var store = new SourceCleanupStore(Path.Combine(directory, "state.db"));
                await store.UpsertAsync(job);
                using var cancellation = new CancellationTokenSource();
                var file = new Mock<IFile>();
                file.Setup(f => f.Exists(source)).Returns(true);
                file.Setup(f => f.OpenRead(source)).Returns(() =>
                {
                    cancellation.Cancel();
                    throw new OperationCanceledException(cancellation.Token);
                });
                var fs = new Mock<IFileSystem>();
                fs.SetupGet(f => f.File).Returns(file.Object);
                fs.SetupGet(f => f.FileInfo).Returns(new System.IO.Abstractions.FileSystem().FileInfo);
                var worker = new SourceCleanupWorker(store, fs.Object,
                    new SourceCleanupOptions { Enabled = true }, NullLogger<SourceCleanupWorker>.Instance);
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => worker.ProcessDueJobsAsync(cancellation.Token));
                var retained = await store.GetActiveAsync(source, job.Fingerprint);
                Assert.Equal(0, retained!.AttemptCount);
                Assert.Null(retained.LastError);
                Assert.True(File.Exists(source));
            }
            finally { DeleteDirectory(directory); }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_DeletesSourceAndRemovesCompletedJob()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "one.dcm");
                File.WriteAllText(sourcePath, "content");
                var fingerprint = FingerprintFor(sourcePath);
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    await store.UpsertAsync(CreateJob(directory, sourcePath, fingerprint));
                    var worker = CreateWorker(store);
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.False(File.Exists(sourcePath));
                    Assert.Null(await store.GetActiveAsync(sourcePath, fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_ExhaustedCleanupMovesSourceToFailureDirectory()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "one.dcm");
                File.WriteAllText(sourcePath, "content");
                var failureDirectory = Path.Combine(directory, "failed");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    job.FailureDirectory = failureDirectory;
                    job.Action = SourceCleanupJobAction.Move;
                    var invalidMoveTarget = Path.Combine(directory, "invalid-target");
                    Directory.CreateDirectory(invalidMoveTarget);
                    job.MoveTargetPath = invalidMoveTarget;
                    job.MaxAttempts = 1;
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store);
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.False(File.Exists(sourcePath));
                    Assert.True(File.Exists(Path.Combine(failureDirectory, "watcher", "one.dcm")));
                    Assert.Null(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_GlobalFileWatcherDisabled_DoesNotProcessJobs()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "disabled.dcm");
                File.WriteAllText(sourcePath, "content");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store, globalEnabled: false);
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.True(File.Exists(sourcePath));
                    Assert.NotNull(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_GlobalFileWatcherDisabled_DoesNotPruneTerminalJobs()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "disabled-terminal.dcm");
                File.WriteAllText(sourcePath, "content");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    job.Action = SourceCleanupJobAction.Keep;
                    job.State = SourceCleanupJobState.Failed;
                    job.NextAttemptUtc = null;
                    job.UpdatedAtUtc = DateTime.UtcNow.AddDays(-2);
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store, globalEnabled: false);
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.NotNull(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_NoWatcherConfigurations_DoesNotProcessJobs()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "no-watcher.dcm");
                File.WriteAllText(sourcePath, "content");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store, watchers: Array.Empty<FileWatcherConfiguration>());
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.True(File.Exists(sourcePath));
                    Assert.NotNull(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_AllWatcherConfigurationsDisabled_DoesNotProcessJobs()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "disabled-watcher.dcm");
                File.WriteAllText(sourcePath, "content");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store, watchers: new[]
                    {
                        new FileWatcherConfiguration { WatcherId = "watcher", Enabled = false }
                    });
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.True(File.Exists(sourcePath));
                    Assert.NotNull(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_GlobalWatcherAndConfigurationEnabled_ProcessesJobs()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "enabled.dcm");
                File.WriteAllText(sourcePath, "content");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store, globalEnabled: true,
                        watchers: new[] { new FileWatcherConfiguration { WatcherId = "watcher", Enabled = true } });
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.False(File.Exists(sourcePath));
                    Assert.Null(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_WhenBacklogExceedsConcurrency_DrainsAllCurrentlyDueJobs()
        {
            var directory = CreateDirectory();
            try
            {
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var sourcePaths = new[]
                    {
                        Path.Combine(directory, "one.dcm"),
                        Path.Combine(directory, "two.dcm"),
                        Path.Combine(directory, "three.dcm")
                    };
                    foreach (var sourcePath in sourcePaths)
                    {
                        File.WriteAllText(sourcePath, sourcePath);
                        await store.UpsertAsync(CreateJob(directory, sourcePath, FingerprintFor(sourcePath)));
                    }

                    var worker = new SourceCleanupWorker(
                        store,
                        new System.IO.Abstractions.FileSystem(),
                        new SourceCleanupOptions { Enabled = true, MaxConcurrentActions = 1 },
                        NullLogger<SourceCleanupWorker>.Instance);

                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.All(sourcePaths, sourcePath => Assert.False(File.Exists(sourcePath)));
                    Assert.Empty(await store.GetDueAsync(DateTime.UtcNow.AddMinutes(1), 10));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_StaleImportReservation_PreservesTransactionIdentity()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "interrupted-import.dcm");
                File.WriteAllText(sourcePath, "content");
                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                    job.FileKey = string.Empty;
                    job.ImportOperationId = "interrupted-operation";
                    job.State = SourceCleanupJobState.Importing;
                    job.NextAttemptUtc = DateTime.UtcNow.AddMinutes(-20);
                    job.UpdatedAtUtc = DateTime.UtcNow.AddMinutes(-20);
                    await store.UpsertAsync(job);

                    var worker = CreateWorker(store);
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.True(File.Exists(sourcePath));
                    var active = await store.GetActiveAsync(sourcePath, job.Fingerprint);
                    Assert.NotNull(active);
                    Assert.Equal("interrupted-operation", active!.ImportOperationId);
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_MovePendingJob_DoesNotMoveReplacementContent()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "reused.dcm");
                File.WriteAllText(sourcePath, "original");
                var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                job.State = SourceCleanupJobState.MovePending;
                job.FailureDirectory = Path.Combine(directory, "failed");
                File.WriteAllText(sourcePath, "replacement-content");

                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    await store.UpsertAsync(job);
                    var worker = CreateWorker(store);
                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.True(File.Exists(sourcePath));
                    Assert.False(File.Exists(Path.Combine(job.FailureDirectory!, "watcher", "reused.dcm")));
                    Assert.Null(await store.GetActiveAsync(sourcePath, job.Fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_SameMetadataDifferentContent_DoesNotDeleteReplacement()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "same-metadata.dcm");
                File.WriteAllText(sourcePath, "original");
                var originalInfo = new System.IO.FileInfo(sourcePath);
                var fingerprint = FingerprintFor(sourcePath);
                File.WriteAllText(sourcePath, "replace!");
                File.SetLastWriteTimeUtc(sourcePath, originalInfo.LastWriteTimeUtc);

                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var job = CreateJob(directory, sourcePath, fingerprint);
                    await store.UpsertAsync(job);
                    var worker = CreateWorker(store);

                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    Assert.True(File.Exists(sourcePath));
                    Assert.Null(await store.GetActiveAsync(sourcePath, fingerprint));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        [Fact]
        public async Task ProcessDueJobsAsync_ExhaustedCleanupWithoutFailureDirectory_SuppressesFurtherAttempts()
        {
            var directory = CreateDirectory();
            try
            {
                var sourcePath = Path.Combine(directory, "no-quarantine.dcm");
                File.WriteAllText(sourcePath, "content");
                var job = CreateJob(directory, sourcePath, FingerprintFor(sourcePath));
                job.Action = SourceCleanupJobAction.Move;
                job.MoveTargetPath = directory;
                job.FailureDirectory = null;
                job.MaxAttempts = 1;

                using (var store = new SourceCleanupStore(Path.Combine(directory, "state.db")))
                {
                    var jobId = await store.UpsertAsync(job);
                    var worker = CreateWorker(store);

                    await worker.ProcessDueJobsAsync(CancellationToken.None);

                    var active = await store.GetActiveAsync(sourcePath, job.Fingerprint);
                    Assert.NotNull(active);
                    Assert.Equal(jobId, active!.Id);
                    Assert.Equal(SourceCleanupJobState.Failed, active.State);
                    Assert.Empty(await store.GetDueAsync(DateTime.UtcNow.AddMinutes(1), 10));
                    Assert.True(File.Exists(sourcePath));
                }
            }
            finally
            {
                DeleteDirectory(directory);
            }
        }

        private static SourceCleanupWorker CreateWorker(
            ISourceCleanupStore store,
            bool? globalEnabled = null,
            IEnumerable<FileWatcherConfiguration>? watchers = null)
        {
            if (!globalEnabled.HasValue && watchers == null)
            {
                return new SourceCleanupWorker(
                    store,
                    new System.IO.Abstractions.FileSystem(),
                    new SourceCleanupOptions { Enabled = true, MaxConcurrentActions = 1 },
                    NullLogger<SourceCleanupWorker>.Instance);
            }

            var optionsManager = new Mock<IFileWatcherOptionsManager>();
            optionsManager
                .Setup(manager => manager.GetOptionsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FileWatcherOptions { Enabled = globalEnabled ?? true });
            var fileWatcher = new Mock<IFileWatcher>();
            fileWatcher
                .Setup(watcher => watcher.GetAllWatchersAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(watchers ?? new[] { new FileWatcherConfiguration { WatcherId = "watcher" } });

            return new SourceCleanupWorker(
                store,
                new System.IO.Abstractions.FileSystem(),
                new SourceCleanupOptions { Enabled = true, MaxConcurrentActions = 1 },
                NullLogger<SourceCleanupWorker>.Instance,
                fileWatcher: fileWatcher.Object,
                fileWatcherOptionsManager: optionsManager.Object);
        }

        private static SourceCleanupJob CreateJob(string directory, string sourcePath, string fingerprint)
        {
            return new SourceCleanupJob
            {
                WatcherId = "watcher",
                TenantId = "tenant",
                SourcePath = sourcePath,
                Fingerprint = fingerprint,
                FileKey = "file-1",
                Action = SourceCleanupJobAction.Delete,
                RetryInitialDelay = TimeSpan.Zero,
                RetryMaxDelay = TimeSpan.Zero,
                NextAttemptUtc = DateTime.UtcNow
            };
        }

        private static string FingerprintFor(string path)
        {
            var info = new System.IO.FileInfo(path);
            using (var sha256 = SHA256.Create())
            using (var stream = File.OpenRead(path))
                return $"fp:v3:{info.Length}:{info.LastWriteTimeUtc.Ticks}:{info.CreationTimeUtc.Ticks}:{Convert.ToBase64String(sha256.ComputeHash(stream))}";
        }

        private static string CreateDirectory()
        {
            var path = Path.Combine(Path.GetTempPath(), "locus-source-cleanup-tests", Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(path);
            return path;
        }

        private static void DeleteDirectory(string path)
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
    }
}
