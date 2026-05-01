using System;
using System.Collections.Concurrent;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class MetadataRepositoryTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly MetadataRepository _repository;
        private readonly string _metadataDir;
        private readonly string _tenantId = "tenant-001";

        public MetadataRepositoryTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            var testId = Guid.NewGuid().ToString("N").Substring(0, 8);
            _metadataDir = Path.Combine(Path.GetTempPath(), $"locus-test-metadata-{testId}");
            _fileSystem.Directory.CreateDirectory(_metadataDir);

            var logger = new Mock<ILogger<MetadataRepository>>();
            _repository = new MetadataRepository(_fileSystem, logger.Object, _metadataDir);
        }

        [Fact]
        public async Task GetNextPendingFileAsync_DecrementsPendingCounter()
        {
            var file = CreateMetadata("file-001", FileProcessingStatus.Pending);
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            Assert.Equal(1, GetPendingCount(_repository, _tenantId));

            var allocated = await _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);

            Assert.NotNull(allocated);
            Assert.Equal(FileProcessingStatus.Processing, allocated!.Status);
            Assert.Equal(0, GetPendingCount(_repository, _tenantId));
        }

        [Fact]
        public async Task GetNextPendingBatchAsync_DecrementsPendingCounterForAllocatedFiles()
        {
            await _repository.AddOrUpdateAsync(CreateMetadata("file-001", FileProcessingStatus.Pending), CancellationToken.None);
            await _repository.AddOrUpdateAsync(CreateMetadata("file-002", FileProcessingStatus.Pending), CancellationToken.None);
            await _repository.AddOrUpdateAsync(CreateMetadata("file-003", FileProcessingStatus.Pending), CancellationToken.None);

            Assert.Equal(3, GetPendingCount(_repository, _tenantId));

            var allocated = await _repository.GetNextPendingBatchAsync(_tenantId, 2, CancellationToken.None);

            Assert.Equal(2, allocated.Count());
            Assert.Equal(1, GetPendingCount(_repository, _tenantId));
        }

        [Fact]
        public async Task TryResetTimedOutFileAsync_IncrementsPendingCounter()
        {
            var processingStart = DateTime.UtcNow.AddMinutes(-10);
            var file = CreateMetadata("file-001", FileProcessingStatus.Processing);
            file.ProcessingStartTime = processingStart;
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            Assert.Equal(0, GetPendingCount(_repository, _tenantId));

            // Use the modern per-file API: first discover timed-out files, then reset each one.
            var cutoff = DateTime.UtcNow.AddMinutes(-1);
            var timedOut = await _repository.GetProcessingTimedOutAsync(_tenantId, cutoff, 10, CancellationToken.None);

            Assert.Single(timedOut);

            var reset = await _repository.TryResetTimedOutFileAsync(
                _tenantId,
                timedOut[0].FileKey,
                timedOut[0].ProcessingStartTime!.Value,
                DateTime.UtcNow,
                CancellationToken.None);

            Assert.True(reset);
            Assert.Equal(1, GetPendingCount(_repository, _tenantId));
        }

        [Fact]
        public async Task TryResetTimedOutFileAsync_ReturnsFalseWhenRecordWasRemovedConcurrently()
        {
            var file = CreateMetadata("file-reset-race", FileProcessingStatus.Processing);
            file.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-15);
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            var timedOut = await _repository.GetProcessingTimedOutAsync(
                _tenantId,
                DateTime.UtcNow.AddMinutes(-1),
                1,
                CancellationToken.None);
            Assert.Single(timedOut);

            await _repository.RemoveAsync(_tenantId, file.FileKey, CancellationToken.None);

            var reset = await _repository.TryResetTimedOutFileAsync(
                _tenantId,
                file.FileKey,
                timedOut[0].ProcessingStartTime!.Value,
                DateTime.UtcNow,
                CancellationToken.None);

            Assert.False(reset);
            Assert.Null(await _repository.GetAsync(_tenantId, file.FileKey, CancellationToken.None));
        }

        [Fact]
        public async Task TryRemovePermanentlyFailedFileAsync_ReturnsFalseWhenRecordChangedConcurrently()
        {
            var file = CreateMetadata("file-remove-race", FileProcessingStatus.PermanentlyFailed);
            file.LastFailedAt = DateTime.UtcNow.AddDays(-10);
            await _repository.AddOrUpdateAsync(file, CancellationToken.None);

            var candidates = await _repository.GetPermanentlyFailedOlderThanAsync(
                _tenantId,
                DateTime.UtcNow.AddDays(-1),
                1,
                CancellationToken.None);
            Assert.Single(candidates);

            var updated = candidates[0].Clone();
            updated.Status = FileProcessingStatus.Pending;
            updated.LastFailedAt = null;
            await _repository.AddOrUpdateAsync(updated, CancellationToken.None);

            var removed = await _repository.TryRemovePermanentlyFailedFileAsync(
                _tenantId,
                file.FileKey,
                candidates[0].LastFailedAt!.Value,
                CancellationToken.None);

            Assert.False(removed);

            var current = await _repository.GetAsync(_tenantId, file.FileKey, CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Pending, current!.Status);
        }

        [Fact]
        public async Task GetAllTenantIdsAsync_IncludesInMemoryTenantWithoutDbFile()
        {
            const string inMemoryTenant = "tenant-memory-only";
            var activeFiles = GetActiveFiles(_repository);
            activeFiles.TryAdd(inMemoryTenant, new ConcurrentDictionary<string, FileMetadata>());

            var tenantIds = (await _repository.GetAllTenantIdsAsync(CancellationToken.None)).ToList();

            Assert.Contains(inMemoryTenant, tenantIds);
        }

        [Fact]
        public async Task GetAllTenantIdsAsync_EnumeratesSubdirectoriesAsTenantIds()
        {
            // Each tenant gets its own subdirectory; only directory names are returned as tenant IDs.
            const string tenantA = "tenant-subdir-a";
            const string tenantB = "tenant-subdir-b";
            _fileSystem.Directory.CreateDirectory(Path.Combine(_metadataDir, tenantA));
            _fileSystem.Directory.CreateDirectory(Path.Combine(_metadataDir, tenantB));
            // A stray file in the root dir should not appear as a tenant ID.
            _fileSystem.File.WriteAllText(Path.Combine(_metadataDir, "stray.db"), "data");

            var tenantIds = (await _repository.GetAllTenantIdsAsync(CancellationToken.None)).ToList();

            Assert.Contains(tenantA, tenantIds);
            Assert.Contains(tenantB, tenantIds);
            Assert.DoesNotContain("stray", tenantIds);
            Assert.DoesNotContain("stray.db", tenantIds);
        }

        [Fact]
        public async Task GetAllTenantIdsAsync_RefreshesDirectorySnapshotAfterCacheWindow()
        {
            const string tenantA = "tenant-cache-a";
            const string tenantB = "tenant-cache-b";

            _fileSystem.Directory.CreateDirectory(Path.Combine(_metadataDir, tenantA));
            var first = (await _repository.GetAllTenantIdsAsync(CancellationToken.None)).ToList();
            Assert.Contains(tenantA, first);

            _fileSystem.Directory.CreateDirectory(Path.Combine(_metadataDir, tenantB));
            await Task.Delay(1100);

            var second = (await _repository.GetAllTenantIdsAsync(CancellationToken.None)).ToList();
            Assert.Contains(tenantB, second);
        }

        [Fact]
        public async Task GetAllTenantIdsAsync_IncludesInMemoryTenantWithinDirectoryCacheWindow()
        {
            const string memoryTenant = "tenant-hot-memory";

            var first = (await _repository.GetAllTenantIdsAsync(CancellationToken.None)).ToList();
            Assert.DoesNotContain(memoryTenant, first);

            var activeFiles = GetActiveFiles(_repository);
            activeFiles.TryAdd(memoryTenant, new ConcurrentDictionary<string, FileMetadata>());

            var second = (await _repository.GetAllTenantIdsAsync(CancellationToken.None)).ToList();
            Assert.Contains(memoryTenant, second);
        }

        [Fact]
        public async Task ProjectionBatch_FailedDeleteFlush_RollsBackInMemoryProjection()
        {
            const string fileKey = "batch-delete-rollback";
            var metadata = CreateMetadata(fileKey, FileProcessingStatus.Completed);
            metadata.CompletedAt = DateTime.UtcNow;
            await InvokeAddOrUpdateDirectAsync(_repository, metadata);

            var batch = BeginProjectionBatch(_repository, _tenantId);
            var removed = await InvokeBatchRemoveProjectedFileAsync(batch, fileKey);
            Assert.True(removed);

            DisposeTenantDatabaseConnection(_repository, _tenantId);

            await Assert.ThrowsAnyAsync<Exception>(() => InvokeBatchFlushAsync(batch));

            var current = await _repository.GetAsync(_tenantId, fileKey, CancellationToken.None);
            var persisted = ReadPersistedMetadata(_metadataDir, _tenantId, fileKey);

            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Completed, current!.Status);
            Assert.NotNull(persisted);
            Assert.Equal(FileProcessingStatus.Completed, persisted!.Status);
        }

        [Fact]
        public async Task ProjectionBatch_FailedUpsertFlush_RollsBackInMemoryProjection()
        {
            const string fileKey = "batch-upsert-rollback";
            var metadata = CreateMetadata(fileKey, FileProcessingStatus.Pending);
            Assert.Null(await _repository.GetAsync(_tenantId, fileKey, CancellationToken.None));

            var batch = BeginProjectionBatch(_repository, _tenantId);
            await InvokeBatchUpsertProjectedFileAsync(batch, metadata);

            DisposeTenantDatabaseConnection(_repository, _tenantId);

            await Assert.ThrowsAnyAsync<Exception>(() => InvokeBatchFlushAsync(batch));

            Assert.Null(await _repository.GetAsync(_tenantId, fileKey, CancellationToken.None));
            Assert.Null(ReadPersistedMetadata(_metadataDir, _tenantId, fileKey));
        }

        [Fact]
        public async Task GetByFileKeyAsync_ReturnsMetadataAcrossTenants()
        {
            await _repository.AddOrUpdateAsync(CreateMetadata("file-tenant-1", FileProcessingStatus.Pending, "tenant-001"), CancellationToken.None);
            await _repository.AddOrUpdateAsync(CreateMetadata("file-tenant-2", FileProcessingStatus.Pending, "tenant-002"), CancellationToken.None);

            var result = await _repository.GetByFileKeyAsync("file-tenant-2", CancellationToken.None);

            Assert.NotNull(result);
            Assert.Equal("tenant-002", result!.TenantId);
            Assert.Equal("file-tenant-2", result.FileKey);
        }

        [Fact]
        public async Task GetByFileKeyAsync_ReturnsDetachedClone()
        {
            var original = CreateMetadata("file-clone", FileProcessingStatus.Pending);
            await _repository.AddOrUpdateAsync(original, CancellationToken.None);

            var snapshot = await _repository.GetByFileKeyAsync("file-clone", CancellationToken.None);
            Assert.NotNull(snapshot);

            snapshot!.Status = FileProcessingStatus.PermanentlyFailed;
            snapshot.LastError = "mutated-outside";

            var current = await _repository.GetAsync(_tenantId, "file-clone", CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Pending, current!.Status);
            Assert.Null(current.LastError);
        }

        [Fact]
        public async Task GetByTenantAsync_ReturnsDetachedClones()
        {
            var original = CreateMetadata("tenant-clone", FileProcessingStatus.Pending);
            await _repository.AddOrUpdateAsync(original, CancellationToken.None);

            var tenantEntries = (await _repository.GetByTenantAsync(_tenantId, CancellationToken.None)).ToList();
            var snapshot = Assert.Single(tenantEntries);
            snapshot.Status = FileProcessingStatus.Processing;
            snapshot.LastError = "mutated-outside";

            var current = await _repository.GetAsync(_tenantId, "tenant-clone", CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Pending, current!.Status);
            Assert.Null(current.LastError);
        }

        [Fact]
        public async Task GetByFileKeyAsync_ReturnsNullAfterRemove()
        {
            await _repository.AddOrUpdateAsync(CreateMetadata("file-remove", FileProcessingStatus.Pending), CancellationToken.None);
            await _repository.RemoveAsync(_tenantId, "file-remove", CancellationToken.None);

            var result = await _repository.GetByFileKeyAsync("file-remove", CancellationToken.None);

            Assert.Null(result);
        }

        [Fact]
        public async Task ExistsByPhysicalPathAsync_ReturnsTrueForIndexedPath()
        {
            var metadata = CreateMetadata("file-path", FileProcessingStatus.Pending);
            metadata.PhysicalPath = Path.Combine(_metadataDir, "tenant-001", "file-path.dat");

            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            var exists = await _repository.ExistsByPhysicalPathAsync(
                _tenantId,
                metadata.PhysicalPath,
                CancellationToken.None);

            Assert.True(exists);
        }

        [Fact]
        public async Task ExistsByPhysicalPathAsync_TracksPathUpdateAndRemove()
        {
            var metadata = CreateMetadata("file-move", FileProcessingStatus.Pending);
            var oldPath = Path.Combine(_metadataDir, "tenant-001", "old", "file-move.dat");
            var newPath = Path.Combine(_metadataDir, "tenant-001", "new", "file-move.dat");

            metadata.PhysicalPath = oldPath;
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            metadata.PhysicalPath = newPath;
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            var oldExists = await _repository.ExistsByPhysicalPathAsync(_tenantId, oldPath, CancellationToken.None);
            var newExists = await _repository.ExistsByPhysicalPathAsync(_tenantId, newPath, CancellationToken.None);
            Assert.False(oldExists);
            Assert.True(newExists);

            await _repository.RemoveAsync(_tenantId, metadata.FileKey, CancellationToken.None);
            var existsAfterRemove = await _repository.ExistsByPhysicalPathAsync(_tenantId, newPath, CancellationToken.None);
            Assert.False(existsAfterRemove);
        }

        [Fact]
        public async Task ExistsByPhysicalPathAsync_RespectsPlatformCaseSensitivity()
        {
            var metadata = CreateMetadata("file-case", FileProcessingStatus.Pending);
            var actualPath = Path.Combine(_metadataDir, "tenant-001", "CaseFile.dat");
            var mismatchedCasePath = Path.Combine(_metadataDir, "tenant-001", "casefile.dat");

            metadata.PhysicalPath = actualPath;
            await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);

            var exists = await _repository.ExistsByPhysicalPathAsync(_tenantId, mismatchedCasePath, CancellationToken.None);

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                Assert.True(exists);
            else
                Assert.False(exists);
        }

        [Fact]
        public async Task GetWritePathStatisticsSnapshot_TracksProjectionEnqueueSubphases()
        {
            var diagnostics = Assert.IsAssignableFrom<IQueueProjectionWritePathDiagnostics>(_repository);

            var pending = CreateMetadata("diag-pending", FileProcessingStatus.Pending);
            await _repository.AddOrUpdateAsync(pending, CancellationToken.None);

            var processing = CreateMetadata("diag-processing", FileProcessingStatus.Processing);
            processing.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-2);
            await _repository.AddOrUpdateAsync(processing, CancellationToken.None);

            var snapshot = diagnostics.GetWritePathStatisticsSnapshot();
            Assert.Equal(2, snapshot.ProjectedWriteCount);
            Assert.Equal(2, snapshot.ValidationCount);
            Assert.Equal(2, snapshot.CacheAndIndexCount);
            Assert.Equal(2, snapshot.CacheMutationCount);
            Assert.Equal(2, snapshot.PhysicalPathIndexCount);
            Assert.Equal(2, snapshot.PendingQueueUpdateCount);
            Assert.Equal(2, snapshot.StatusIndexUpdateCount);
            Assert.Equal(2, snapshot.PersistenceEnqueueCount);
        }

        [Fact]
        public async Task GetNextPendingFileAsync_BoundedDequeuesEventuallyAllocatesUnderStaleQueue()
        {
            var metadata = CreateMetadata("stale-heavy", FileProcessingStatus.Pending);
            for (int i = 0; i < 700; i++)
            {
                metadata.CreatedAt = DateTime.UtcNow.AddTicks(i);
                await _repository.AddOrUpdateAsync(metadata.Clone(), CancellationToken.None);
            }

            FileMetadata? allocated = null;
            for (int i = 0; i < 20 && allocated == null; i++)
            {
                allocated = await _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
            }

            Assert.NotNull(allocated);
            Assert.Equal(FileProcessingStatus.Processing, allocated!.Status);
        }

        [Fact]
        public async Task GetNextPendingFileAsync_ConcurrentAllocators_DoNotDuplicateAllocations()
        {
            const int fileCount = 128;
            for (var i = 0; i < fileCount; i++)
            {
                var metadata = CreateMetadata($"concurrent-single-{i:D3}", FileProcessingStatus.Pending);
                metadata.CreatedAt = DateTime.UtcNow.AddTicks(i);
                await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            var allocations = await Task.WhenAll(
                Enumerable.Range(0, fileCount)
                    .Select(_ => _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None)));

            var allocated = allocations.Where(x => x != null).Select(x => x!).ToList();
            Assert.Equal(fileCount, allocated.Count);
            Assert.Equal(fileCount, allocated.Select(x => x.FileKey).Distinct().Count());
            Assert.All(allocated, x => Assert.Equal(FileProcessingStatus.Processing, x.Status));
        }

        [Fact]
        public async Task GetNextPendingFileAsync_UnderLockContention_PrefetchesAndBatchCanDrainRemaining()
        {
            const int fileCount = 4;
            for (var i = 0; i < fileCount; i++)
            {
                var metadata = CreateMetadata($"prefetch-{i:D2}", FileProcessingStatus.Pending);
                metadata.CreatedAt = DateTime.UtcNow.AddTicks(i);
                await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            var tenantLocks = GetTenantLocks(_repository);
            var tenantLock = tenantLocks.GetOrAdd(_tenantId, _ => new SemaphoreSlim(1, 1));

            await tenantLock.WaitAsync();
            var firstTask = _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
            Assert.False(firstTask.IsCompleted);
            tenantLock.Release();

            var first = await firstTask;
            Assert.NotNull(first);
            Assert.Equal(0, GetPendingCount(_repository, _tenantId));

            var remaining = (await _repository.GetNextPendingBatchAsync(_tenantId, fileCount - 1, CancellationToken.None)).ToList();
            Assert.Equal(fileCount - 1, remaining.Count);

            var allocated = remaining.Prepend(first!).ToList();
            Assert.Equal(fileCount, allocated.Select(x => x.FileKey).Distinct().Count());
            Assert.All(allocated, x => Assert.Equal(FileProcessingStatus.Processing, x.Status));
        }

        [Fact]
        public async Task GetNextPendingFileAsync_PrefetchStaleEntry_IsDiscardedAfterTimeoutReset()
        {
            const int fileCount = 4;
            for (var i = 0; i < fileCount; i++)
            {
                var metadata = CreateMetadata($"prefetch-stale-{i:D2}", FileProcessingStatus.Pending);
                metadata.CreatedAt = DateTime.UtcNow.AddTicks(i);
                await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            var tenantLocks = GetTenantLocks(_repository);
            var tenantLock = tenantLocks.GetOrAdd(_tenantId, _ => new SemaphoreSlim(1, 1));

            await tenantLock.WaitAsync();
            var firstTask = _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
            Assert.False(firstTask.IsCompleted);
            tenantLock.Release();

            var first = await firstTask;
            Assert.NotNull(first);
            Assert.Equal("prefetch-stale-00", first!.FileKey);

            var staleCandidate = await _repository.GetAsync(_tenantId, "prefetch-stale-01", CancellationToken.None);
            Assert.NotNull(staleCandidate);
            Assert.Equal(FileProcessingStatus.Processing, staleCandidate!.Status);
            Assert.NotNull(staleCandidate.ProcessingStartTime);

            var reset = await _repository.TryResetTimedOutFileAsync(
                _tenantId,
                staleCandidate.FileKey,
                staleCandidate.ProcessingStartTime!.Value,
                DateTime.UtcNow,
                CancellationToken.None);
            Assert.True(reset);

            var allocated = await _repository.GetNextPendingFileAsync(_tenantId, CancellationToken.None);
            Assert.NotNull(allocated);

            var current = await _repository.GetAsync(_tenantId, allocated!.FileKey, CancellationToken.None);
            Assert.NotNull(current);
            Assert.Equal(FileProcessingStatus.Processing, current!.Status);
            Assert.Equal(allocated.ProcessingStartTime, current.ProcessingStartTime);
        }

        [Fact]
        public async Task GetNextPendingBatchAsync_ConcurrentAllocators_DoNotOverlapFileKeys()
        {
            const int batchSize = 8;
            const int workerCount = 16;
            var totalFiles = batchSize * workerCount;

            for (var i = 0; i < totalFiles; i++)
            {
                var metadata = CreateMetadata($"concurrent-batch-{i:D3}", FileProcessingStatus.Pending);
                metadata.CreatedAt = DateTime.UtcNow.AddTicks(i);
                await _repository.AddOrUpdateAsync(metadata, CancellationToken.None);
            }

            var batches = await Task.WhenAll(
                Enumerable.Range(0, workerCount)
                    .Select(_ => _repository.GetNextPendingBatchAsync(_tenantId, batchSize, CancellationToken.None)));

            var allocated = batches.SelectMany(x => x).ToList();
            Assert.Equal(totalFiles, allocated.Count);
            Assert.Equal(totalFiles, allocated.Select(x => x.FileKey).Distinct().Count());
            Assert.All(allocated, x => Assert.Equal(FileProcessingStatus.Processing, x.Status));
        }

        [Fact]
        public async Task GetProcessingTimedOutAsync_DoesNotRepeatAfterStatusTransition()
        {
            var oldA = CreateMetadata("proc-a", FileProcessingStatus.Processing);
            oldA.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-30);
            var oldB = CreateMetadata("proc-b", FileProcessingStatus.Processing);
            oldB.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-20);
            var oldC = CreateMetadata("proc-c", FileProcessingStatus.Processing);
            oldC.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-10);

            await _repository.AddOrUpdateAsync(oldA, CancellationToken.None);
            await _repository.AddOrUpdateAsync(oldB, CancellationToken.None);
            await _repository.AddOrUpdateAsync(oldC, CancellationToken.None);

            var cutoff = DateTime.UtcNow.AddMinutes(-5);
            var firstBatch = await _repository.GetProcessingTimedOutAsync(_tenantId, cutoff, 2, CancellationToken.None);
            Assert.Equal(2, firstBatch.Count);

            foreach (var item in firstBatch)
            {
                var updated = item.Clone();
                updated.Status = FileProcessingStatus.Pending;
                updated.ProcessingStartTime = null;
                await _repository.AddOrUpdateAsync(updated, CancellationToken.None);
            }

            var secondBatch = await _repository.GetProcessingTimedOutAsync(_tenantId, cutoff, 2, CancellationToken.None);
            Assert.Single(secondBatch);
            Assert.Equal("proc-c", secondBatch[0].FileKey);
        }

        [Fact]
        public async Task GetProcessingTimedOutAsync_ReturnsOldestFirst()
        {
            var first = CreateMetadata("order-a", FileProcessingStatus.Processing);
            first.CreatedAt = DateTime.UtcNow.AddMinutes(-40);
            first.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-30);
            var second = CreateMetadata("order-b", FileProcessingStatus.Processing);
            second.CreatedAt = DateTime.UtcNow.AddMinutes(-39);
            second.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-20);
            var third = CreateMetadata("order-c", FileProcessingStatus.Processing);
            third.CreatedAt = DateTime.UtcNow.AddMinutes(-38);
            third.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-10);

            await _repository.AddOrUpdateAsync(second, CancellationToken.None);
            await _repository.AddOrUpdateAsync(third, CancellationToken.None);
            await _repository.AddOrUpdateAsync(first, CancellationToken.None);

            var cutoff = DateTime.UtcNow.AddMinutes(-5);
            var batch = await _repository.GetProcessingTimedOutAsync(_tenantId, cutoff, 3, CancellationToken.None);

            Assert.Equal(["order-a", "order-b", "order-c"], batch.Select(x => x.FileKey).ToArray());
        }

        [Fact]
        public async Task GetProcessingTimedOutAsync_DoesNotDuplicateWithinSingleBatch()
        {
            var metadata = CreateMetadata("proc-dup", FileProcessingStatus.Processing);
            metadata.ProcessingStartTime = DateTime.UtcNow.AddMinutes(-15);

            for (int i = 0; i < 12; i++)
            {
                await _repository.AddOrUpdateAsync(metadata.Clone(), CancellationToken.None);
            }

            var cutoff = DateTime.UtcNow.AddMinutes(-5);
            var batch = await _repository.GetProcessingTimedOutAsync(_tenantId, cutoff, 10, CancellationToken.None);

            Assert.Single(batch);
            Assert.Equal("proc-dup", batch[0].FileKey);
        }

        [Fact]
        public async Task GetPermanentlyFailedOlderThanAsync_DoesNotRepeatAfterRemove()
        {
            var oldA = CreateMetadata("failed-a", FileProcessingStatus.PermanentlyFailed);
            oldA.LastFailedAt = DateTime.UtcNow.AddDays(-10);
            var oldB = CreateMetadata("failed-b", FileProcessingStatus.PermanentlyFailed);
            oldB.LastFailedAt = DateTime.UtcNow.AddDays(-9);
            var oldC = CreateMetadata("failed-c", FileProcessingStatus.PermanentlyFailed);
            oldC.LastFailedAt = DateTime.UtcNow.AddDays(-8);

            await _repository.AddOrUpdateAsync(oldA, CancellationToken.None);
            await _repository.AddOrUpdateAsync(oldB, CancellationToken.None);
            await _repository.AddOrUpdateAsync(oldC, CancellationToken.None);

            var cutoff = DateTime.UtcNow.AddDays(-7);
            var firstBatch = await _repository.GetPermanentlyFailedOlderThanAsync(_tenantId, cutoff, 2, CancellationToken.None);
            Assert.Equal(2, firstBatch.Count);

            foreach (var item in firstBatch)
            {
                await _repository.RemoveAsync(_tenantId, item.FileKey, CancellationToken.None);
            }

            var secondBatch = await _repository.GetPermanentlyFailedOlderThanAsync(_tenantId, cutoff, 2, CancellationToken.None);
            Assert.Single(secondBatch);
            Assert.Equal("failed-c", secondBatch[0].FileKey);
        }

        [Fact]
        public async Task AddOrUpdateAsync_WhenPersistenceChannelIsSaturated_UsesCoalescingFallback()
        {
            var logger = new Mock<ILogger<MetadataRepository>>();
            var saturatedDir = Path.Combine(_metadataDir, $"saturated-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(saturatedDir);
            var saturatedRepo = new MetadataRepository(
                _fileSystem,
                logger.Object,
                saturatedDir,
                enableBackgroundPersistence: true,
                maxDrainBatchSize: 1,
                persistenceQueueSoftMergeThresholdPercent: 100,
                maxPersistenceQueueSize: 1);

            try
            {
                var metadata = CreateMetadata("coalesce-file", FileProcessingStatus.Pending);
                for (int i = 0; i < 10_000; i++)
                {
                    metadata.RetryCount = i;
                    await saturatedRepo.AddOrUpdateAsync(metadata.Clone(), CancellationToken.None);
                }

                await Task.Delay(300);
                var coalesceLogCounter = GetPrivateIntField(saturatedRepo, "_coalescedLogCounter");
                var coalescedDepth = GetPrivateIntField(saturatedRepo, "_coalescedDepth");
                Assert.True(coalesceLogCounter > 0 || coalescedDepth > 0);
            }
            finally
            {
                saturatedRepo.Dispose();
                SqliteConnection.ClearAllPools();
                if (_fileSystem.Directory.Exists(saturatedDir))
                    _fileSystem.Directory.Delete(saturatedDir, recursive: true);
            }
        }

        [Fact]
        public async Task AddOrUpdateAsync_WhenSqliteWriteFails_RequeuesFailedBatchForRetry()
        {
            var logger = new Mock<ILogger<MetadataRepository>>();
            var blockedDir = Path.Combine(_metadataDir, $"blocked-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(blockedDir);

            var blockedRepo = new MetadataRepository(
                _fileSystem,
                logger.Object,
                blockedDir,
                enableBackgroundPersistence: true,
                maxDrainBatchSize: 1,
                persistenceIntervalSeconds: 1);

            var tenantId = "tenant-blocked";
            var tenantDir = Path.Combine(blockedDir, tenantId);
            _fileSystem.Directory.CreateDirectory(tenantDir);
            var dbPath = Path.Combine(tenantDir, "metadata.db");

            try
            {
                _fileSystem.Directory.CreateDirectory(dbPath);
                var metadata = CreateMetadata("retry-file", FileProcessingStatus.Pending, tenantId);
                await blockedRepo.AddOrUpdateAsync(metadata, CancellationToken.None);

                var observedRequeue = SpinWait.SpinUntil(
                    () => GetPrivateIntField(blockedRepo, "_coalescedDepth") > 0,
                    TimeSpan.FromSeconds(3));

                Assert.True(observedRequeue);
            }
            finally
            {
                blockedRepo.Dispose();
                SqliteConnection.ClearAllPools();
                if (_fileSystem.Directory.Exists(blockedDir))
                    _fileSystem.Directory.Delete(blockedDir, recursive: true);
            }
        }

        [Fact]
        public void DrainPersistenceBatch_WhenQueuedOperationIsOlderThanCoalescedOperation_PersistsLatestOperation()
        {
            var logger = new Mock<ILogger<MetadataRepository>>();
            var orderedDir = Path.Combine(_metadataDir, $"ordered-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(orderedDir);
            var tenantId = "tenant-ordered";
            var fileKey = "ordered-file";

            var repository = new MetadataRepository(
                _fileSystem,
                logger.Object,
                orderedDir,
                enableBackgroundPersistence: false,
                maxDrainBatchSize: 2,
                maxPersistenceQueueSize: 2);

            try
            {
                var older = CreateMetadata(fileKey, FileProcessingStatus.Pending, tenantId);
                older.RetryCount = 1;

                var newer = older.Clone();
                newer.Status = FileProcessingStatus.Completed;
                newer.RetryCount = 2;
                newer.CompletedAt = DateTime.UtcNow;

                Assert.True(InvokeTryWritePersistenceOperation(repository, older));
                InvokeCoalescePersistenceOperation(repository, newer);

                Assert.True(InvokeDrainPersistenceBatch(repository));

                var persisted = ReadPersistedMetadata(orderedDir, tenantId, fileKey);
                Assert.NotNull(persisted);
                Assert.Equal(FileProcessingStatus.Completed, persisted!.Status);
                Assert.Equal(2, persisted.RetryCount);
            }
            finally
            {
                repository.Dispose();
                SqliteConnection.ClearAllPools();
                if (_fileSystem.Directory.Exists(orderedDir))
                    _fileSystem.Directory.Delete(orderedDir, recursive: true);
            }
        }

        [Fact]
        public void DrainPersistenceBatch_WhenCoalescedOperationIsOlderThanQueuedOperation_PersistsLatestOperation()
        {
            var logger = new Mock<ILogger<MetadataRepository>>();
            var orderedDir = Path.Combine(_metadataDir, $"ordered-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(orderedDir);
            var tenantId = "tenant-ordered";
            var fileKey = "ordered-file";

            var repository = new MetadataRepository(
                _fileSystem,
                logger.Object,
                orderedDir,
                enableBackgroundPersistence: false,
                maxDrainBatchSize: 2,
                maxPersistenceQueueSize: 2);

            try
            {
                var older = CreateMetadata(fileKey, FileProcessingStatus.Pending, tenantId);
                older.RetryCount = 1;

                var newer = older.Clone();
                newer.Status = FileProcessingStatus.Completed;
                newer.RetryCount = 2;
                newer.CompletedAt = DateTime.UtcNow;

                InvokeCoalescePersistenceOperation(repository, older);
                Assert.True(InvokeTryWritePersistenceOperation(repository, newer));

                Assert.True(InvokeDrainPersistenceBatch(repository));

                var persisted = ReadPersistedMetadata(orderedDir, tenantId, fileKey);
                Assert.NotNull(persisted);
                Assert.Equal(FileProcessingStatus.Completed, persisted!.Status);
                Assert.Equal(2, persisted.RetryCount);
            }
            finally
            {
                repository.Dispose();
                SqliteConnection.ClearAllPools();
                if (_fileSystem.Directory.Exists(orderedDir))
                    _fileSystem.Directory.Delete(orderedDir, recursive: true);
            }
        }

        [Fact]
        public async Task StartupLoad_BatchedPendingLoad_PreservesCreatedAtOrder()
        {
            var logger = new Mock<ILogger<MetadataRepository>>();
            var tenantId = "tenant-startup-batch";
            var startupDir = Path.Combine(_metadataDir, $"startup-{Guid.NewGuid():N}");
            _fileSystem.Directory.CreateDirectory(startupDir);

            var createdTimes = Enumerable.Range(0, 6)
                .Select(i => DateTime.UtcNow.AddMinutes(-10).AddSeconds(i))
                .ToArray();
            var insertionOrder = new[] { 3, 1, 5, 0, 4, 2 };

            using (var writer = new MetadataRepository(
                _fileSystem,
                logger.Object,
                startupDir,
                enableBackgroundPersistence: false,
                startupLoadBatchSize: 2))
            {
                foreach (var index in insertionOrder)
                {
                    var metadata = CreateMetadata($"startup-file-{index:D2}", FileProcessingStatus.Pending, tenantId);
                    metadata.CreatedAt = createdTimes[index];
                    await writer.AddOrUpdateAsync(metadata, CancellationToken.None);
                }
            }

            using (var reader = new MetadataRepository(
                _fileSystem,
                logger.Object,
                startupDir,
                enableBackgroundPersistence: false,
                startupLoadBatchSize: 2))
            {
                var allocated = new System.Collections.Generic.List<string>();
                for (int i = 0; i < insertionOrder.Length; i++)
                {
                    var next = await reader.GetNextPendingFileAsync(tenantId, CancellationToken.None);
                    Assert.NotNull(next);
                    allocated.Add(next!.FileKey);
                }

                var expected = createdTimes
                    .Select((createdAt, index) => new { createdAt, index })
                    .OrderBy(x => x.createdAt)
                    .Select(x => $"startup-file-{x.index:D2}")
                    .ToArray();

                Assert.Equal(expected, allocated);
            }
        }

        [Fact]
        public void Constructor_ThrowsWhenStartupLoadBatchSizeIsNotPositive()
        {
            var logger = new Mock<ILogger<MetadataRepository>>();

            Assert.Throws<ArgumentException>(() => new MetadataRepository(
                _fileSystem,
                logger.Object,
                _metadataDir,
                enableBackgroundPersistence: false,
                startupLoadBatchSize: 0));
        }

        public void Dispose()
        {
            _repository.Dispose();
            SqliteConnection.ClearAllPools();

            try
            {
                if (_fileSystem.Directory.Exists(_metadataDir))
                    _fileSystem.Directory.Delete(_metadataDir, recursive: true);
            }
            catch
            {
                // Ignore cleanup failures in tests.
            }
        }

        private FileMetadata CreateMetadata(string fileKey, FileProcessingStatus status, string? tenantId = null)
        {
            return new FileMetadata
            {
                FileKey = fileKey,
                TenantId = tenantId ?? _tenantId,
                VolumeId = "vol-001",
                PhysicalPath = $"/test/{fileKey}.dat",
                DirectoryPath = "/",
                FileSize = 1024,
                Status = status,
                CreatedAt = DateTime.UtcNow
            };
        }

        private static int GetPendingCount(MetadataRepository repository, string tenantId)
        {
            var field = typeof(MetadataRepository).GetField("_pendingFileCounts", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);

            var counters = (ConcurrentDictionary<string, int>)field!.GetValue(repository)!;
            return counters.TryGetValue(tenantId, out var count) ? count : 0;
        }

        private static ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>> GetActiveFiles(MetadataRepository repository)
        {
            var field = typeof(MetadataRepository).GetField("_activeFiles", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);

            return (ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>>)field!.GetValue(repository)!;
        }

        private static Task InvokeAddOrUpdateDirectAsync(MetadataRepository repository, FileMetadata metadata)
        {
            var method = typeof(MetadataRepository).GetMethod(
                "AddOrUpdateDirectAsync",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            return (Task)method!.Invoke(repository, new object[] { metadata, CancellationToken.None })!;
        }

        private static object BeginProjectionBatch(MetadataRepository repository, string tenantId)
        {
            var method = typeof(MetadataRepository).GetMethod(
                "BeginProjectionBatch",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            return method!.Invoke(repository, new object[] { tenantId })!;
        }

        private static Task InvokeBatchUpsertProjectedFileAsync(object batch, FileMetadata metadata)
        {
            var method = batch.GetType().GetMethod("UpsertProjectedFileAsync");
            Assert.NotNull(method);

            return (Task)method!.Invoke(batch, new object[] { metadata, CancellationToken.None })!;
        }

        private static Task<bool> InvokeBatchRemoveProjectedFileAsync(object batch, string fileKey)
        {
            var method = batch.GetType().GetMethod("RemoveProjectedFileAsync");
            Assert.NotNull(method);

            return (Task<bool>)method!.Invoke(batch, new object[] { fileKey, CancellationToken.None })!;
        }

        private static Task InvokeBatchFlushAsync(object batch)
        {
            var method = batch.GetType().GetMethod("FlushAsync");
            Assert.NotNull(method);

            try
            {
                return (Task)method!.Invoke(batch, new object[] { CancellationToken.None })!;
            }
            catch (TargetInvocationException ex) when (ex.InnerException != null)
            {
                return Task.FromException(ex.InnerException);
            }
        }

        private static void DisposeTenantDatabaseConnection(MetadataRepository repository, string tenantId)
        {
            var field = typeof(MetadataRepository).GetField("_databases", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);

            var databases = (ConcurrentDictionary<string, Lazy<SqliteConnection>>)field!.GetValue(repository)!;
            Assert.True(databases.TryGetValue(tenantId, out var lazyConnection));
            Assert.True(lazyConnection!.IsValueCreated);

            lazyConnection.Value.Dispose();
        }

        private static ConcurrentDictionary<string, SemaphoreSlim> GetTenantLocks(MetadataRepository repository)
        {
            var field = typeof(MetadataRepository).GetField("_tenantLocks", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);

            return (ConcurrentDictionary<string, SemaphoreSlim>)field!.GetValue(repository)!;
        }

        private static int GetPrivateIntField(MetadataRepository repository, string fieldName)
        {
            var field = typeof(MetadataRepository).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);

            return (int)field!.GetValue(repository)!;
        }

        private static bool InvokeTryWritePersistenceOperation(MetadataRepository repository, FileMetadata metadata)
        {
            var operation = CreatePersistenceOperation(repository, metadata);
            var method = typeof(MetadataRepository).GetMethod(
                "TryWritePersistenceOperation",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            return (bool)method!.Invoke(repository, new[] { operation })!;
        }

        private static void InvokeCoalescePersistenceOperation(MetadataRepository repository, FileMetadata metadata)
        {
            var operation = CreatePersistenceOperation(repository, metadata);
            var method = typeof(MetadataRepository).GetMethod(
                "CoalescePersistenceOperation",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            method!.Invoke(repository, new[] { operation, 0, "test" });
        }

        private static bool InvokeDrainPersistenceBatch(MetadataRepository repository)
        {
            var method = typeof(MetadataRepository).GetMethod(
                "DrainPersistenceBatch",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            return (bool)method!.Invoke(repository, Array.Empty<object>())!;
        }

        private static object CreatePersistenceOperation(MetadataRepository repository, FileMetadata metadata)
        {
            var method = typeof(MetadataRepository).GetMethod(
                "CreatePersistenceUpsert",
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(method);

            return method!.Invoke(repository, new object[] { metadata })!;
        }

        private static FileMetadata? ReadPersistedMetadata(string metadataDirectory, string tenantId, string fileKey)
        {
            var dbPath = Path.Combine(metadataDirectory, tenantId, "metadata.db");
            using var connection = new SqliteConnection($"Data Source={dbPath}");
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = @"
SELECT file_key, tenant_id, volume_id, physical_path, directory_path, file_size,
       created_at, status, retry_count, last_failed_at, last_error,
       processing_start_time, completed_at, delete_succeeded_at, dead_lettered_at,
       available_for_processing_at, original_file_name, file_extension, metadata_json
FROM files
WHERE file_key = $file_key;";
            command.Parameters.AddWithValue("$file_key", fileKey);

            using var reader = command.ExecuteReader();
            if (!reader.Read())
                return null;

            return new FileMetadata
            {
                FileKey = reader.GetString(0),
                TenantId = reader.GetString(1),
                VolumeId = reader.GetString(2),
                PhysicalPath = reader.GetString(3),
                DirectoryPath = reader.GetString(4),
                FileSize = reader.GetInt64(5),
                CreatedAt = DateTime.Parse(reader.GetString(6), null, System.Globalization.DateTimeStyles.RoundtripKind),
                Status = (FileProcessingStatus)reader.GetInt32(7),
                RetryCount = reader.GetInt32(8),
                LastFailedAt = reader.IsDBNull(9) ? null : DateTime.Parse(reader.GetString(9), null, System.Globalization.DateTimeStyles.RoundtripKind),
                LastError = reader.IsDBNull(10) ? null : reader.GetString(10),
                ProcessingStartTime = reader.IsDBNull(11) ? null : DateTime.Parse(reader.GetString(11), null, System.Globalization.DateTimeStyles.RoundtripKind),
                CompletedAt = reader.IsDBNull(12) ? null : DateTime.Parse(reader.GetString(12), null, System.Globalization.DateTimeStyles.RoundtripKind),
                DeleteSucceededAt = reader.IsDBNull(13) ? null : DateTime.Parse(reader.GetString(13), null, System.Globalization.DateTimeStyles.RoundtripKind),
                DeadLetteredAt = reader.IsDBNull(14) ? null : DateTime.Parse(reader.GetString(14), null, System.Globalization.DateTimeStyles.RoundtripKind),
                AvailableForProcessingAt = reader.IsDBNull(15) ? null : DateTime.Parse(reader.GetString(15), null, System.Globalization.DateTimeStyles.RoundtripKind),
                OriginalFileName = reader.IsDBNull(16) ? null : reader.GetString(16),
                FileExtension = reader.IsDBNull(17) ? null : reader.GetString(17),
            };
        }
    }
}
