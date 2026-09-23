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
