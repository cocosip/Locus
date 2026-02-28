using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;
using LiteDBOptions = Locus.Core.Models.LiteDBOptions;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Repository for file metadata storage using per-tenant LiteDB with active-data in-memory caching.
    /// Only keeps Pending/Processing/Failed files in memory. Completed files are immediately deleted.
    ///
    /// Write-Behind architecture: AddOrUpdateAsync and RemoveAsync update in-memory cache first,
    /// then enqueue LiteDB persistence asynchronously. If LiteDB crashes, file writes continue
    /// uninterrupted. On process restart, orphaned physical files are recovered by the cleanup service.
    ///
    /// Thread-safe for concurrent access.
    /// </summary>
    public class MetadataRepository : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<MetadataRepository> _logger;
        private readonly string _metadataDirectory;
        private readonly LiteDBOptions _liteDbOptions;

        // Per-tenant in-memory cache (only active files: Pending/Processing/Failed)
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>> _activeFiles;

        // Global fileKey -> tenantId index for O(1) cross-tenant lookup by file key.
        private readonly ConcurrentDictionary<string, string> _fileKeyTenantIndex;

        // Per-tenant LiteDB databases (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<LiteDatabase>> _databases;

        // Per-tenant locks for concurrent file allocation.
        // NOTE: Locks are intentionally retained for the lifetime of the repository even if a
        // tenant is deleted (no RemoveTenantAsync path here). The set of tenants in production
        // systems is typically small and stable, so the bounded growth (~1 SemaphoreSlim per
        // tenant, ~72 bytes each) is acceptable. If the system supports frequent tenant churn,
        // consider sweeping _tenantLocks in a maintenance task after verifying no thread holds
        // the lock (check SemaphoreSlim.CurrentCount == 1).
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;

        // Per-tenant FIFO queue of Pending file keys for O(1) allocation.
        // Enqueued when a file becomes Pending; dequeued-and-validated in GetNextPendingFileAsync.
        // Entries are validated on dequeue (stale entries from status changes are skipped).
        // Queue maintains arrival order so callers get FIFO scheduling without any sorting.
        private readonly ConcurrentDictionary<string, ConcurrentQueue<string>> _pendingKeys;

        // Per-tenant count of actual Pending files (maintained in AddOrUpdateAsync / RemoveAsync).
        // Allows CompactPendingQueues to decide whether a queue is bloated in O(1) rather than
        // scanning the entire cache to count Pending entries.
        // Access via Interlocked only.
        private readonly ConcurrentDictionary<string, int> _pendingFileCounts;

        // --- Write-Behind: async LiteDB persistence (netstandard2.0 compatible) ---
        // Memory is always updated first. LiteDB writes are decoupled via this queue.
        // If LiteDB is unavailable, writes buffer in memory and are retried by the background loop.
        // On crash, in-flight writes are lost — physical files stay on disk as orphans and are
        // recovered by the cleanup service on next startup.
        private readonly ConcurrentQueue<PersistenceOperation> _persistenceQueue;
        private readonly SemaphoreSlim _persistenceSignal; // one permit per enqueued item
        private readonly Task _persistenceTask;
        private readonly CancellationTokenSource _persistenceCts;
        private readonly bool _enableBackgroundPersistence;

        private bool _disposed;

        // Discriminated union for channel messages
        private readonly struct PersistenceOperation
        {
            public readonly bool IsDelete;
            public readonly FileMetadata? Metadata;  // set when IsDelete == false
            public readonly string TenantId;
            public readonly string FileKey;

            public PersistenceOperation(FileMetadata metadata)
            {
                IsDelete = false;
                Metadata = metadata;
                TenantId = metadata.TenantId;
                FileKey = metadata.FileKey;
            }

            public PersistenceOperation(string tenantId, string fileKey)
            {
                IsDelete = true;
                Metadata = null;
                TenantId = tenantId;
                FileKey = fileKey;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepository"/> class.
        /// </summary>
        public MetadataRepository(
            IFileSystem fileSystem,
            ILogger<MetadataRepository> logger,
            string metadataDirectory,
            LiteDBOptions? liteDbOptions = null,
            bool enableBackgroundPersistence = true)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(metadataDirectory))
                throw new ArgumentException("Metadata directory cannot be empty", nameof(metadataDirectory));

            _metadataDirectory = metadataDirectory;
            _liteDbOptions = liteDbOptions ?? new LiteDBOptions();
            _enableBackgroundPersistence = enableBackgroundPersistence;
            _activeFiles = new ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>>();
            _fileKeyTenantIndex = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
            _databases = new ConcurrentDictionary<string, Lazy<LiteDatabase>>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _pendingKeys = new ConcurrentDictionary<string, ConcurrentQueue<string>>();
            _pendingFileCounts = new ConcurrentDictionary<string, int>();

            // Write-behind queue + semaphore (fields initialized here, task started LAST)
            _persistenceQueue = new ConcurrentQueue<PersistenceOperation>();
            _persistenceSignal = new SemaphoreSlim(0, int.MaxValue);
            _persistenceCts = new CancellationTokenSource();

            // Ensure metadata directory exists before starting the background task,
            // so that if CreateDirectory throws, the task is never started and cannot be orphaned.
            if (!_fileSystem.Directory.Exists(_metadataDirectory))
            {
                _fileSystem.Directory.CreateDirectory(_metadataDirectory);
            }

            _logger.LogInformation(
                "MetadataRepository initialized at {Directory} with LiteDB write-behind: Journal={Journal}, Checkpoint={Checkpoint}, Timeout={Timeout}s",
                _metadataDirectory,
                _liteDbOptions.EnableJournal,
                _liteDbOptions.CheckpointInterval,
                _liteDbOptions.TimeoutSeconds);

            if (_enableBackgroundPersistence)
            {
                // Start background persistence loop LAST — all fields are fully initialized at this point.
                // The task immediately blocks on _persistenceSignal.WaitAsync() and consumes no CPU
                // until EnqueuePersistence() is called. Dispose() cancels _persistenceCts and waits
                // for the task to drain remaining items before closing databases.
                _persistenceTask = Task.Run(() => RunPersistenceLoopAsync(_persistenceCts.Token));
            }
            else
            {
                _persistenceTask = Task.CompletedTask;
                _logger.LogInformation("MetadataRepository write-behind background loop is disabled; persistence is synchronous.");
            }
        }

        /// <summary>
        /// Gets or creates a LiteDB database for a tenant.
        /// Auto-recovery: Rebuilds corrupted database on initialization failure.
        /// </summary>
        private LiteDatabase GetDatabase(string tenantId)
        {
            // Use Lazy<LiteDatabase> to ensure thread-safe initialization
            // This prevents multiple threads from simultaneously creating the database instance
            var lazyDb = _databases.GetOrAdd(tenantId, tid => new Lazy<LiteDatabase>(() =>
            {
                try
                {
                    // Ensure metadata directory exists (thread-safe in case of concurrent access)
                    if (!_fileSystem.Directory.Exists(_metadataDirectory))
                    {
                        _fileSystem.Directory.CreateDirectory(_metadataDirectory);
                    }

                    var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tid}.db");
                    // Use configured LiteDB options (WAL mode strongly recommended for K8s/network storage)
                    var connectionString = _liteDbOptions.BuildConnectionString(dbPath);
                    var db = new LiteDatabase(connectionString);

                    var files = db.GetCollection<FileMetadata>("files");
                    // FileKey is already BsonId, no need to create additional index
                    files.EnsureIndex(x => x.Status);
                    files.EnsureIndex(x => x.CreatedAt);
                    files.EnsureIndex(x => x.AvailableForProcessingAt);

                    _logger.LogDebug("Created/opened LiteDB database for tenant {TenantId} at {Path}", tid, dbPath);

                    // Load active files into memory on first access
                    LoadActiveFilesForTenant(tid, files);

                    return db;
                }
                catch (LiteException ex) when (ex.Message.Contains("ReadFull") || ex.Message.Contains("PAGE_SIZE") || ex.Message.Contains("Checkpoint"))
                {
                    // Database is corrupted during initialization - attempt automatic recovery.
                    // IMPORTANT: Do NOT call BeginDatabaseRebuildAsync here — it acquires _tenantLocks[tid],
                    // which another thread may already hold while waiting for this Lazy to complete,
                    // causing a deadlock (Lazy lock ↔ tenant lock cycle).
                    // Instead call the lock-free internal helper directly.
                    _logger.LogError(ex, "CORRUPTED METADATA DATABASE DETECTED for tenant {TenantId} during initialization. Attempting automatic recovery...", tid);

                    try
                    {
                        // Remove from cache to force rebuild
                        _databases.TryRemove(tid, out _);
                        if (_activeFiles.TryRemove(tid, out var staleTenantCache))
                            RemoveTenantFromGlobalIndex(staleTenantCache);

                        // Rebuild database without acquiring the tenant lock (we may be called
                        // from within the Lazy factory which is already locked via ExecutionAndPublication).
                        var backupPath = RebuildDatabaseFileNoLock(tid);

                        _logger.LogWarning("Metadata database rebuilt for tenant {TenantId} during initialization. Backup: {BackupPath}", tid, backupPath ?? "N/A");

                        // Retry initialization with configured LiteDB options
                        var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tid}.db");
                        var connectionString = _liteDbOptions.BuildConnectionString(dbPath);
                        var db = new LiteDatabase(connectionString);

                        var files = db.GetCollection<FileMetadata>("files");
                        files.EnsureIndex(x => x.Status);
                        files.EnsureIndex(x => x.CreatedAt);
                        files.EnsureIndex(x => x.AvailableForProcessingAt);

                        _logger.LogInformation("Successfully recovered and initialized metadata database for tenant {TenantId}", tid);

                        return db;
                    }
                    catch (Exception recoveryEx)
                    {
                        _logger.LogError(recoveryEx, "Failed to recover corrupted metadata database for tenant {TenantId} during initialization", tid);
                        throw; // Re-throw if recovery fails
                    }
                }
            }, LazyThreadSafetyMode.ExecutionAndPublication));

            // Access the Value property to trigger initialization if needed
            return lazyDb.Value;
        }

        /// <summary>
        /// Loads active files (Pending/Processing/Failed) into memory for a tenant.
        /// </summary>
        private void LoadActiveFilesForTenant(string tenantId, ILiteCollection<FileMetadata> collection)
        {
            var activeStatuses = new[]
            {
                FileProcessingStatus.Pending,
                FileProcessingStatus.Processing,
                FileProcessingStatus.Failed,
                FileProcessingStatus.PermanentlyFailed
            };

            var activeFiles = collection.Find(f => activeStatuses.Contains(f.Status)).ToList();

            if (activeFiles.Count > 0)
            {
                var cache = _activeFiles.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, FileMetadata>());
                var pendingQueue = _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<string>());

                foreach (var file in activeFiles)
                {
                    cache[file.FileKey] = file;
                    _fileKeyTenantIndex[file.FileKey] = tenantId;
                }

                // Enqueue Pending files in CreatedAt order to restore FIFO scheduling after restart.
                int pendingCount = 0;
                foreach (var file in activeFiles.Where(f => f.Status == FileProcessingStatus.Pending)
                                                .OrderBy(f => f.CreatedAt))
                {
                    pendingQueue.Enqueue(file.FileKey);
                    pendingCount++;
                }

                // Seed the O(1) pending count so CompactPendingQueues can check bloat without scanning.
                _pendingFileCounts[tenantId] = pendingCount;

                _logger.LogInformation("Loaded {Count} active files for tenant {TenantId} into memory",
                    activeFiles.Count, tenantId);
            }
        }

        /// <summary>
        /// Gets the in-memory cache for a tenant.
        /// </summary>
        private ConcurrentDictionary<string, FileMetadata> GetCache(string tenantId)
        {
            // Ensure database is initialized (which also loads active files)
            GetDatabase(tenantId);
            return _activeFiles.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, FileMetadata>());
        }

        /// <summary>
        /// Adds or updates file metadata.
        /// Write-Behind: updates in-memory cache immediately (never fails from caller's perspective),
        /// then enqueues LiteDB persistence asynchronously via background loop.
        /// If LiteDB is unavailable, the write is queued and retried; the file on disk remains safe.
        /// On process crash before flush, the physical file becomes an orphan recovered by cleanup service.
        /// </summary>
        /// <remarks>
        /// For maintenance operations (database rebuild) that require immediate LiteDB persistence,
        /// use <see cref="AddOrUpdateDirectAsync"/> instead.
        /// </remarks>
        public Task AddOrUpdateAsync(FileMetadata metadata, CancellationToken ct)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.FileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(metadata));

            // 1. Update in-memory cache FIRST — always succeeds, immediately visible to readers.
            //    Capture the previous status (if any) to maintain the O(1) pending count accurately.
            var cache = _activeFiles.GetOrAdd(metadata.TenantId,
                _ => new ConcurrentDictionary<string, FileMetadata>());

            cache.TryGetValue(metadata.FileKey, out var previous);
            cache[metadata.FileKey] = metadata;
            _fileKeyTenantIndex[metadata.FileKey] = metadata.TenantId;

            // 2. Maintain _pendingFileCounts: +1 when transitioning INTO Pending, -1 when leaving.
            //    This keeps CompactPendingQueues O(1) instead of O(N).
            bool wasP = previous?.Status == FileProcessingStatus.Pending;
            bool isP  = metadata.Status == FileProcessingStatus.Pending;
            if (!wasP && isP)
                _pendingFileCounts.AddOrUpdate(metadata.TenantId, 1, (_, c) => c + 1);
            else if (wasP && !isP)
                _pendingFileCounts.AddOrUpdate(metadata.TenantId, 0, (_, c) => Math.Max(0, c - 1));

            // 3. Enqueue file key when it becomes Pending so the allocator can find it in O(1).
            // Non-Pending transitions are not explicitly removed from the queue; stale entries
            // are validated and skipped when GetNextPendingFileAsync dequeues them.
            if (isP)
            {
                var pendingQueue = _pendingKeys.GetOrAdd(metadata.TenantId, _ => new ConcurrentQueue<string>());
                pendingQueue.Enqueue(metadata.FileKey);
            }

            _logger.LogDebug("Added/updated metadata for file: {FileKey}, Tenant: {TenantId}, Status: {Status}",
                metadata.FileKey, metadata.TenantId, metadata.Status);

            // Queue LiteDB persistence — caller is not blocked; background loop drains the queue.
            EnqueuePersistence(new PersistenceOperation(metadata));

            return Task.CompletedTask;
        }

        /// <summary>
        /// Adds or updates file metadata with immediate synchronous LiteDB write.
        /// Bypasses the Write-Behind queue — use only for maintenance operations such as
        /// database rebuild where the caller needs the data persisted before returning.
        /// </summary>
        internal Task AddOrUpdateDirectAsync(FileMetadata metadata, CancellationToken ct)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.FileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(metadata));

            // Update in-memory cache.
            // Maintain _pendingFileCounts for the same reason as AddOrUpdateAsync, so that
            // CompactPendingQueues has an accurate O(1) bloat estimate even for files added
            // via this direct path (e.g., orphan recovery in StorageCleanupService).
            var cache = _activeFiles.GetOrAdd(metadata.TenantId,
                _ => new ConcurrentDictionary<string, FileMetadata>());
            cache.TryGetValue(metadata.FileKey, out var previous);
            cache[metadata.FileKey] = metadata;
            _fileKeyTenantIndex[metadata.FileKey] = metadata.TenantId;

            bool wasP = previous?.Status == FileProcessingStatus.Pending;
            bool isP  = metadata.Status == FileProcessingStatus.Pending;
            if (!wasP && isP)
                _pendingFileCounts.AddOrUpdate(metadata.TenantId, 1, (_, c) => c + 1);
            else if (wasP && !isP)
                _pendingFileCounts.AddOrUpdate(metadata.TenantId, 0, (_, c) => Math.Max(0, c - 1));

            // Enqueue when Pending — stale entries skipped on dequeue (same as AddOrUpdateAsync).
            if (isP)
            {
                var pendingQueue = _pendingKeys.GetOrAdd(metadata.TenantId, _ => new ConcurrentQueue<string>());
                pendingQueue.Enqueue(metadata.FileKey);
            }

            // Write directly to LiteDB (synchronous — required for rebuild correctness)
            var db = GetDatabase(metadata.TenantId);
            var files = db.GetCollection<FileMetadata>("files");
            files.Upsert(metadata);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Gets file metadata by file key.
        /// All queries hit memory first (microseconds).
        /// </summary>
        public Task<FileMetadata?> GetAsync(string tenantId, string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var cache = GetCache(tenantId);
            cache.TryGetValue(fileKey, out var metadata);
            return Task.FromResult<FileMetadata?>(metadata);
        }

        /// <summary>
        /// Removes file metadata by file key.
        /// Used when file processing is completed successfully.
        /// </summary>
        public Task<bool> RemoveAsync(string tenantId, string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Remove from memory cache
            var cache = GetCache(tenantId);
            var removed = cache.TryRemove(fileKey, out var removedMetadata);

            if (removed)
            {
                _fileKeyTenantIndex.TryRemove(fileKey, out _);

                // Maintain _pendingFileCounts: if the removed file was Pending, decrement.
                if (removedMetadata?.Status == FileProcessingStatus.Pending)
                    _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));

                // No explicit removal from _pendingKeys: ConcurrentQueue does not support
                // O(1) keyed removal. The stale entry (if any) will be skipped by the
                // dequeue-and-validate loop in GetNextPendingFileAsync.

                // Queue LiteDB deletion — caller is not blocked by disk I/O
                EnqueuePersistence(new PersistenceOperation(tenantId, fileKey));

                _logger.LogDebug("Removed metadata for file: {FileKey}, Tenant: {TenantId}", fileKey, tenantId);
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Returns the IDs of all tenants that currently have an active in-memory cache.
        /// Used by maintenance operations to iterate tenants one at a time instead of
        /// loading all files across all tenants into a single collection.
        /// </summary>
        public IEnumerable<string> GetActiveTenantIds() => _activeFiles.Keys;

        /// <summary>
        /// Gets all file metadata for a specific tenant.
        /// </summary>
        public Task<IEnumerable<FileMetadata>> GetByTenantAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var cache = GetCache(tenantId);
            var results = cache.Values.ToList();
            return Task.FromResult<IEnumerable<FileMetadata>>(results);
        }

        /// <summary>
        /// Gets all file metadata with a specific status.
        /// </summary>
        public Task<IEnumerable<FileMetadata>> GetByStatusAsync(FileProcessingStatus status, CancellationToken ct)
        {
            var results = new List<FileMetadata>();

            foreach (var kvp in _activeFiles)
            {
                var tenantFiles = kvp.Value.Values.Where(m => m.Status == status);
                results.AddRange(tenantFiles);
            }

            return Task.FromResult<IEnumerable<FileMetadata>>(results);
        }

        // Maximum consecutive re-enqueues of delayed-retry files allowed per single-file allocation.
        // Kept small (16) to bound the time the tenant lock is held: files are queued in FIFO
        // order, so a delayed file near the head implies most queued files are also delayed.
        // Scanning past 16 delayed entries without finding a ready file is not worth the latency
        // imposed on other concurrent workers waiting for the same lock.
        private const int MaxDelayedReEnqueues = 16;

        // Larger limit used only in batch allocation, where finding more ready files
        // within the same lock acquisition justifies a deeper scan.
        private const int MaxDelayedReEnqueuesBatch = 256;

        /// <summary>
        /// Gets the next pending file for processing (atomic, O(1) amortized).
        /// Returns null if no files are currently available.
        /// Thread-safe: Uses per-tenant lock for atomic dequeue-and-mark.
        ///
        /// Uses a FIFO ConcurrentQueue. Stale entries (files no longer Pending in cache)
        /// are validated and discarded on dequeue. Delayed-retry files (AvailableForProcessingAt
        /// in the future) are re-enqueued to the back of the queue and skipped — but only up to
        /// MaxDelayedReEnqueues times to bound lock hold time under high-contention scenarios.
        /// </summary>
        public async Task<FileMetadata?> GetNextPendingFileAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct);
            try
            {
                var cache = GetCache(tenantId);
                var pendingQueue = _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<string>());
                var now = DateTime.UtcNow;
                int delayedCount = 0;

                while (pendingQueue.TryDequeue(out var key))
                {
                    // Validate: skip stale entries whose file was removed or status changed.
                    if (!cache.TryGetValue(key, out var candidate))
                        continue; // stale — file was removed from cache
                    if (candidate.Status != FileProcessingStatus.Pending)
                        continue; // stale — status changed outside this lock

                    if (candidate.AvailableForProcessingAt.HasValue
                        && candidate.AvailableForProcessingAt.Value > now)
                    {
                        // Delayed retry — not ready yet. Re-enqueue to the back and stop scanning
                        // after MaxDelayedReEnqueues attempts to bound the time this lock is held.
                        // Files are roughly FIFO-ordered; a delayed head implies most items are delayed.
                        pendingQueue.Enqueue(key);
                        if (++delayedCount >= MaxDelayedReEnqueues)
                            break;
                        continue;
                    }

                    // Clone before mutating so concurrent lock-free readers always observe
                    // a fully-consistent object (Pending snapshot or Processing snapshot).
                    var updated = candidate.Clone();
                    updated.Status = FileProcessingStatus.Processing;
                    updated.ProcessingStartTime = DateTime.UtcNow;

                    cache[updated.FileKey] = updated;
                    _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));
                    EnqueuePersistence(new PersistenceOperation(updated));

                    _logger.LogDebug("Allocated file for processing: {FileKey}, Tenant: {TenantId}", updated.FileKey, tenantId);
                    return updated;
                }

                return null;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Gets a batch of pending files for processing (atomic operation).
        /// Thread-safe: Uses the same per-tenant SemaphoreSlim as GetNextPendingFileAsync
        /// to ensure concurrent callers cannot receive the same files.
        /// </summary>
        public async Task<IEnumerable<FileMetadata>> GetNextPendingBatchAsync(
            string tenantId,
            int batchSize,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (batchSize <= 0)
                throw new ArgumentException("Batch size must be greater than zero", nameof(batchSize));

            // Get or create tenant-specific lock (shared with GetNextPendingFileAsync)
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));

            // Acquire lock to ensure atomic dequeue-and-mark across concurrent callers
            await tenantLock.WaitAsync(ct);
            try
            {
                var cache = GetCache(tenantId);
                var pendingQueue = _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<string>());
                var now = DateTime.UtcNow;

                var results = new List<FileMetadata>(batchSize);
                int delayedCount = 0;
                // Cap total dequeues to avoid an unbounded loop when many stale/delayed entries
                // are present. batchSize * 5 tolerates up to 80% stale entries in a typical run.
                int maxDequeues = batchSize * 5 + MaxDelayedReEnqueuesBatch;
                int dequeued = 0;

                while (results.Count < batchSize
                       && dequeued++ < maxDequeues
                       && pendingQueue.TryDequeue(out var key))
                {
                    if (!cache.TryGetValue(key, out var candidate))
                        continue; // stale — removed from cache
                    if (candidate.Status != FileProcessingStatus.Pending)
                        continue; // stale — status changed

                    if (candidate.AvailableForProcessingAt.HasValue
                        && candidate.AvailableForProcessingAt.Value > now)
                    {
                        // Not ready yet — re-enqueue and stop scanning to avoid busy-looping.
                        // Batch allocation tolerates more delayed re-enqueues than single-file
                        // allocation since filling the batch justifies holding the lock longer.
                        pendingQueue.Enqueue(key);
                        if (++delayedCount >= MaxDelayedReEnqueuesBatch)
                            break;
                        continue;
                    }

                    // Clone before mutating — same rationale as GetNextPendingFileAsync.
                    var updated = candidate.Clone();
                    updated.Status = FileProcessingStatus.Processing;
                    updated.ProcessingStartTime = DateTime.UtcNow;

                    cache[updated.FileKey] = updated;
                    _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));
                    EnqueuePersistence(new PersistenceOperation(updated));
                    results.Add(updated);
                }

                _logger.LogDebug("Allocated {Count} files for processing, Tenant: {TenantId}", results.Count, tenantId);
                return results;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Resets files that have been in Processing status for longer than the timeout.
        /// Uses Write-Behind (same as AddOrUpdateAsync) for consistency with the rest of the
        /// repository: memory is updated first, LiteDB persistence is async via the background loop.
        /// </summary>
        public Task<int> ResetTimedOutFilesAsync(TimeSpan timeout, CancellationToken ct)
        {
            var cutoffTime = DateTime.UtcNow - timeout;
            var count = 0;

            foreach (var kvp in _activeFiles)
            {
                var tenantId = kvp.Key;
                var cache = kvp.Value;

                // ConcurrentDictionary.Values enumeration is thread-safe without snapshotting.
                foreach (var file in cache.Values)
                {
                    if (file.Status != FileProcessingStatus.Processing
                        || !file.ProcessingStartTime.HasValue
                        || file.ProcessingStartTime.Value >= cutoffTime)
                        continue;

                    var processingDuration = DateTime.UtcNow - file.ProcessingStartTime.Value;

                    // Clone before mutating — keeps the cache entry consistent for lock-free readers.
                    var updated = file.Clone();
                    updated.Status = FileProcessingStatus.Pending;
                    updated.ProcessingStartTime = null;
                    updated.AvailableForProcessingAt = null;

                    // Atomically replace cache entry then enqueue for LiteDB persistence.
                    cache.TryGetValue(updated.FileKey, out var previous);
                    cache[updated.FileKey] = updated;
                    if (previous?.Status != FileProcessingStatus.Pending)
                        _pendingFileCounts.AddOrUpdate(tenantId, 1, (_, c) => c + 1);
                    EnqueuePersistence(new PersistenceOperation(updated));

                    // Re-enqueue in the pending queue so the allocator can find it immediately.
                    _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<string>()).Enqueue(updated.FileKey);

                    count++;
                    _logger.LogWarning("Reset timed-out file: {FileKey}, Tenant: {TenantId}, was processing for {Duration}",
                        updated.FileKey, tenantId, processingDuration);
                }
            }

            return Task.FromResult(count);
        }

        /// <summary>
        /// Finds file metadata by fileKey using a global O(1) index.
        /// Falls back to a tenant scan only to self-heal stale index entries.
        /// </summary>
        public Task<FileMetadata?> GetByFileKeyAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (_fileKeyTenantIndex.TryGetValue(fileKey, out var tenantId)
                && _activeFiles.TryGetValue(tenantId, out var indexedTenantFiles)
                && indexedTenantFiles.TryGetValue(fileKey, out var indexedMetadata))
            {
                return Task.FromResult<FileMetadata?>(indexedMetadata);
            }

            // Fallback: recover from stale/missing index entries (rare, e.g. after crash recovery).
            foreach (var tenantEntry in _activeFiles)
            {
                if (!tenantEntry.Value.TryGetValue(fileKey, out var metadata))
                    continue;

                _fileKeyTenantIndex[fileKey] = tenantEntry.Key;
                return Task.FromResult<FileMetadata?>(metadata);
            }

            _fileKeyTenantIndex.TryRemove(fileKey, out _);
            return Task.FromResult<FileMetadata?>(null);
        }

        /// <summary>
        /// Gets all file metadata (for maintenance operations).
        /// </summary>
        public Task<IEnumerable<FileMetadata>> GetAllAsync(CancellationToken ct)
        {
            var results = new List<FileMetadata>();

            foreach (var kvp in _activeFiles)
            {
                results.AddRange(kvp.Value.Values);
            }

            return Task.FromResult<IEnumerable<FileMetadata>>(results);
        }

        /// <summary>
        /// Optimizes (rebuilds) a specific tenant's metadata database to reclaim space.
        /// This method is thread-safe and will block all operations for this tenant during optimization.
        /// WARNING: This is a heavy operation. Should be called during maintenance windows.
        /// </summary>
        /// <param name="tenantId">The tenant ID whose database should be optimized.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Tuple of (size before, size after) in bytes.</returns>
        public Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct)
        {
            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tenantId}.db");

            return LiteDbOptimizationHelper.OptimizeDatabaseAsync(
                tenantId,
                dbPath,
                _databases,
                _tenantLocks,
                _fileSystem,
                _logger,
                "metadata",
                ct);
        }

        /// <summary>
        /// Gets all tenant IDs that have metadata databases.
        /// </summary>
        public async Task<IEnumerable<string>> GetAllTenantIdsAsync(CancellationToken ct)
        {
            // Include in-memory tenants first so cleanup can process recent writes that have not
            // been flushed to disk yet by the write-behind persistence loop.
            var tenantIds = new HashSet<string>(_activeFiles.Keys, StringComparer.Ordinal);

            if (_fileSystem.Directory.Exists(_metadataDirectory))
            {
                // Use the file system directly to get the database files
                var dbFiles = _fileSystem.Directory.GetFiles(_metadataDirectory, "*.db");
                var diskNames = dbFiles
                    .Select(f => _fileSystem.Path.GetFileNameWithoutExtension(f))
                    .Where(n => !string.IsNullOrWhiteSpace(n))
                    .ToList();
                var diskNameSet = new HashSet<string>(diskNames, StringComparer.OrdinalIgnoreCase);

                foreach (var fileName in diskNames)
                {
                    if (IsMetadataLogSidecar(fileName, diskNameSet))
                        continue;

                    if (!IsValidTenantDatabaseName(fileName))
                        continue;

                    tenantIds.Add(fileName);
                }
            }

            return await Task.FromResult<IEnumerable<string>>(tenantIds);
        }

        private static bool IsValidTenantDatabaseName(string? fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                return false;

            var tenantDatabaseName = fileName!;

            // Filter LiteDB maintenance sidecar files and backups.
            if (tenantDatabaseName.Contains("-backup", StringComparison.OrdinalIgnoreCase))
                return false;

            if (tenantDatabaseName.Contains(".corrupted.", StringComparison.OrdinalIgnoreCase))
                return false;

            if (tenantDatabaseName.EndsWith("-journal", StringComparison.OrdinalIgnoreCase))
                return false;

            return true;
        }

        private void RemoveTenantFromGlobalIndex(ConcurrentDictionary<string, FileMetadata> tenantCache)
        {
            foreach (var fileKey in tenantCache.Keys)
                _fileKeyTenantIndex.TryRemove(fileKey, out _);
        }

        private static bool IsMetadataLogSidecar(string fileName, HashSet<string> diskNameSet)
        {
            if (!fileName.EndsWith("-log", StringComparison.OrdinalIgnoreCase))
                return false;

            var baseName = fileName.Substring(0, fileName.Length - 4);
            return diskNameSet.Contains(baseName);
        }

        /// <summary>
        /// Backs up and deletes the corrupted database file WITHOUT acquiring the tenant lock.
        /// Must only be called from within the Lazy factory (GetDatabase) where acquiring the
        /// tenant lock would risk a deadlock with threads that hold the lock while waiting for
        /// the Lazy to complete.
        /// </summary>
        /// <returns>Backup file path, or null if no database file existed.</returns>
        private string? RebuildDatabaseFileNoLock(string tenantId)
        {
            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tenantId}.db");

            if (!_fileSystem.File.Exists(dbPath))
            {
                _logger.LogInformation("No database file to rebuild for tenant {TenantId}", tenantId);
                return null;
            }

            // Dispose any existing connection held by a previously created Lazy.
            if (_databases.TryGetValue(tenantId, out var existingLazy) && existingLazy.IsValueCreated)
            {
                try { existingLazy.Value?.Dispose(); }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing database connection during lock-free rebuild for tenant {TenantId}", tenantId);
                }
            }

            _databases.TryRemove(tenantId, out _);
            if (_activeFiles.TryRemove(tenantId, out var staleTenantCache))
                RemoveTenantFromGlobalIndex(staleTenantCache);
            _pendingKeys.TryRemove(tenantId, out _);
            _pendingFileCounts.TryRemove(tenantId, out _);

            var backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
            _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
            _logger.LogInformation("Backed up corrupted database: {BackupPath}", backupPath);

            _fileSystem.File.Delete(dbPath);
            _logger.LogInformation("Deleted corrupted database: {DatabasePath}", dbPath);

            return backupPath;
        }

        /// <summary>
        /// Prepares for database rebuild by safely disposing connections and backing up the database.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations.
        /// IMPORTANT: Caller must call FinishDatabaseRebuildAsync() to release the lock.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Path to the backup file, or null if no database exists.</returns>
        public async Task<string?> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tenantId}.db");

            // Get or create tenant lock (same lock used for all operations)
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));

            // Acquire exclusive lock - this will block all operations for this tenant
            await tenantLock.WaitAsync(ct);

            try
            {
                // If database doesn't exist, nothing to backup
                if (!_fileSystem.File.Exists(dbPath))
                {
                    _logger.LogInformation("No database file to rebuild for tenant {TenantId}", tenantId);
                    return null;
                }

                _logger.LogWarning("Beginning database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);

                // Step 1: Dispose existing database connection
                if (_databases.TryGetValue(tenantId, out var lazyDb) && lazyDb.IsValueCreated)
                {
                    try
                    {
                        lazyDb.Value?.Dispose();
                        _logger.LogDebug("Disposed database connection for rebuild: {TenantId}", tenantId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error disposing database connection for tenant {TenantId}", tenantId);
                    }
                }

                // Step 2: Remove from cache to prevent reconnection
                _databases.TryRemove(tenantId, out _);

                // Step 3: Clear in-memory cache and pending index
                if (_activeFiles.TryRemove(tenantId, out var staleTenantCache))
                    RemoveTenantFromGlobalIndex(staleTenantCache);
                _pendingKeys.TryRemove(tenantId, out _);
                _pendingFileCounts.TryRemove(tenantId, out _);

                // Step 4: Backup corrupted database
                var backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
                _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
                _logger.LogInformation("Backed up corrupted database: {BackupPath}", backupPath);

                // Step 5: Delete corrupted database
                _fileSystem.File.Delete(dbPath);
                _logger.LogInformation("Deleted corrupted database: {DatabasePath}", dbPath);

                return backupPath;
            }
            catch
            {
                // If anything fails, release the lock immediately
                tenantLock.Release();
                throw;
            }

            // NOTE: Lock is NOT released here - it will be released in FinishDatabaseRebuildAsync()
        }

        /// <summary>
        /// Finishes database rebuild by releasing the tenant lock.
        /// IMPORTANT: Must be called after BeginDatabaseRebuildAsync() to unblock operations.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        public void FinishDatabaseRebuild(string tenantId)
        {
            if (_tenantLocks.TryGetValue(tenantId, out var tenantLock))
            {
                tenantLock.Release();
                _logger.LogInformation("Database rebuild completed for tenant {TenantId}. Operations unblocked.", tenantId);
            }
        }

        // Maximum number of operations allowed in the persistence queue.
        // If LiteDB is unavailable for an extended period, the queue will fill up. When full,
        // new operations are dropped with a warning log. The physical file is already safe on
        // disk and will be recovered by the cleanup service on next restart.
        private const int MaxPersistenceQueueSize = 100_000;

        // Compact pending queues every N drain cycles to reclaim memory from stale entries.
        // At ~one drain-cycle per 5 s, 20 cycles ≈ every ~100 seconds.
        // A shorter interval limits queue bloat when files are retried frequently,
        // reducing the window in which a retried file is delayed from being re-allocated.
        private const int CompactionIntervalCycles = 20;

        // Enqueues an operation and wakes the background persistence loop.
        // If the queue is full (LiteDB has been unavailable for a long time), the operation
        // is dropped with a warning — memory state is already correct, so data is not lost
        // within this process lifetime. On restart, the cleanup service reconciles disk vs metadata.
        private void EnqueuePersistence(PersistenceOperation op)
        {
            // Used by unit tests to avoid orphaned background workers.
            // When disabled, persist inline and skip queueing/task signaling.
            if (!_enableBackgroundPersistence)
            {
                ExecuteBatch(op.TenantId, new List<PersistenceOperation> { op });
                return;
            }

            // ConcurrentQueue.Count is O(1). Small TOCTOU race is acceptable: we only
            // need to prevent unbounded growth, not enforce an exact hard limit.
            if (_persistenceQueue.Count >= MaxPersistenceQueueSize)
            {
                if (op.IsDelete)
                    _logger.LogWarning(
                        "Persistence queue is full ({Max} items). Dropping LiteDB delete for {FileKey} (tenant {TenantId}). " +
                        "Stale metadata record will be removed by cleanup service.",
                        MaxPersistenceQueueSize, op.FileKey, op.TenantId);
                else
                    _logger.LogError(
                        "Persistence queue is full ({Max} items). Dropping LiteDB upsert for {FileKey} (tenant {TenantId}). " +
                        "File is safe on disk. On restart, StorageCleanupService will detect the orphan and REBUILD its metadata " +
                        "so it re-enters the Pending queue for processing. Investigate why LiteDB is unavailable.",
                        MaxPersistenceQueueSize, op.FileKey, op.TenantId);
                return;
            }

            _persistenceQueue.Enqueue(op);
            _persistenceSignal.Release(); // wake the background loop
        }

        // Background loop: waits for a signal, then drains the entire queue in one pass,
        // grouping operations by tenant so each tenant gets a single LiteDB transaction
        // instead of one transaction per operation. This dramatically reduces LiteDB overhead
        // under burst writes (e.g. 1000 concurrent file writes → 1 transaction per tenant).
        private async Task RunPersistenceLoopAsync(CancellationToken ct)
        {
            int drainCycles = 0;
            while (true)
            {
                try
                {
                    // Wait for a signal OR up to 5 seconds (whichever comes first).
                    // The periodic timeout guarantees that any items enqueued during low-activity
                    // periods are flushed within ~5 s, reducing data-loss exposure on unexpected
                    // process termination even when the queue never fills.
                    await _persistenceSignal.WaitAsync(TimeSpan.FromSeconds(5), ct);
                }
                catch (OperationCanceledException)
                {
                    break; // shutdown requested — fall through to drain
                }

                // Drain all queued items in a single batch, regardless of how many signals
                // accumulated. Consuming extra semaphore permits avoids spurious wake-ups.
                DrainPersistenceQueue();

                // Periodically compact pending queues to reclaim memory occupied by stale
                // entries left over from completed/failed files that were never explicitly removed.
                if (++drainCycles % CompactionIntervalCycles == 0)
                    CompactPendingQueues();
            }

            // Drain any items enqueued before cancellation was observed.
            DrainPersistenceQueue();
        }

        // Drains all currently queued persistence operations in one pass.
        // Operations are grouped by tenantId and flushed in a single LiteDB transaction
        // per tenant, minimising fsync overhead under high write throughput.
        private void DrainPersistenceQueue()
        {
            // Collect all available items without blocking.
            var batch = new List<PersistenceOperation>();
            while (_persistenceQueue.TryDequeue(out var op))
            {
                batch.Add(op);
                // Consume the matching semaphore permit to stay in sync.
                _persistenceSignal.Wait(0);
            }

            if (batch.Count == 0)
                return;

            // Group by tenantId for per-tenant transaction batching.
            var byTenant = new Dictionary<string, List<PersistenceOperation>>(StringComparer.Ordinal);
            foreach (var op in batch)
            {
                if (!byTenant.TryGetValue(op.TenantId, out var list))
                {
                    list = new List<PersistenceOperation>();
                    byTenant[op.TenantId] = list;
                }
                list.Add(op);
            }

            foreach (var kvp in byTenant)
                ExecuteBatch(kvp.Key, kvp.Value);
        }

        // Writes a batch of operations for a single tenant inside one LiteDB transaction.
        // Errors are logged but never propagated — LiteDB unavailability must never fail writes.
        private void ExecuteBatch(string tenantId, List<PersistenceOperation> ops)
        {
            try
            {
                var db = GetDatabase(tenantId);
                var files = db.GetCollection<FileMetadata>("files");

                db.BeginTrans();
                try
                {
                    foreach (var op in ops)
                    {
                        if (op.IsDelete)
                            files.Delete(op.FileKey);
                        else
                            files.Upsert(op.Metadata!);
                    }
                    db.Commit();

                    _logger.LogDebug("Flushed {Count} persistence operations for tenant {TenantId}", ops.Count, tenantId);
                }
                catch
                {
                    db.Rollback();
                    throw;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "LiteDB batch write failed for tenant {TenantId} ({Count} operations). " +
                    "Physical files are safe on disk; metadata may be lost if process restarts before LiteDB recovers.",
                    tenantId, ops.Count);
            }
        }

        // Rebuilds _pendingKeys for any tenant whose queue is significantly larger than
        // the number of actual Pending files in the cache.  Called periodically by the
        // single background persistence thread — never from a request path.
        //
        // Bloat check uses _pendingFileCounts (O(1)) maintained by AddOrUpdateAsync/RemoveAsync,
        // avoiding the previous O(N) cache scan that blocked the persistence thread.
        //
        // Thread-safety: _pendingKeys[tenantId] is replaced atomically (ConcurrentDictionary
        // write).  Any concurrent GetNextPendingFileAsync call that already holds a reference
        // to the old ConcurrentQueue will continue to dequeue from it safely; all dequeued
        // keys are validated against the authoritative cache before being promoted, so no
        // duplicates or phantom entries can slip through.
        private void CompactPendingQueues()
        {
            foreach (var kvp in _activeFiles)
            {
                var tenantId = kvp.Key;
                var cache = kvp.Value;

                if (!_pendingKeys.TryGetValue(tenantId, out var queue))
                    continue;

                // O(1): read from the counter maintained by AddOrUpdateAsync / RemoveAsync.
                _pendingFileCounts.TryGetValue(tenantId, out var actualPendingCount);

                // Only rebuild when the queue is significantly bloated:
                // more than 2× actual pending entries + a 100-entry slack buffer.
                if (queue.Count <= actualPendingCount * 2 + 100)
                    continue;

                _logger.LogDebug(
                    "Compacting pending queue for tenant {TenantId}: {QueueCount} queue entries → {PendingCount} actual pending",
                    tenantId, queue.Count, actualPendingCount);

                // Rebuild from cache: Pending files only, ordered by CreatedAt to preserve FIFO.
                // This O(N) scan only runs when the queue IS bloated — much less frequently than before.
                var pendingKeys = cache.Values
                    .Where(m => m.Status == FileProcessingStatus.Pending)
                    .OrderBy(m => m.CreatedAt)
                    .Select(m => m.FileKey);

                // TryUpdate atomically replaces the queue only if _pendingKeys[tenantId] still
                // points to the same instance we observed earlier (i.e., `queue`).  This narrows
                // the race window: if a concurrent AddOrUpdateAsync already swapped in a newer
                // queue, we leave it untouched rather than silently discarding newly enqueued keys.
                // Any in-flight GetNextPendingFileAsync holding the old reference drains it
                // harmlessly — every dequeued key is validated against the authoritative cache.
                var newQueue = new ConcurrentQueue<string>(pendingKeys);
                _pendingKeys.TryUpdate(tenantId, newQueue, queue);
            }
        }

        /// <summary>
        /// Disposes the repository and closes all databases.
        /// Signals the write-behind background task to stop, waits up to 5 seconds
        /// for it to drain remaining queued writes, then closes all LiteDB connections.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_enableBackgroundPersistence)
            {
                // 1. Signal the background persistence loop to stop accepting new signals
                _persistenceCts.Cancel();

                // 2. Wait for the background task to drain remaining queued items (up to 5 seconds).
                // On K8s graceful shutdown (SIGTERM), this window allows in-flight writes to reach LiteDB.
                // If LiteDB is unavailable or the queue is too large to drain in time, remaining items
                // are discarded — physical files are safe on disk and recovered by cleanup service on restart.
                try
                {
                    _persistenceTask.Wait(TimeSpan.FromSeconds(5));
                }
                catch (AggregateException)
                {
                    // OperationCanceledException wrapped in AggregateException — expected on cancellation
                }
            }

            // 3. Close all databases AFTER the drain is complete
            foreach (var lazyDb in _databases.Values)
            {
                try
                {
                    if (lazyDb.IsValueCreated)
                        lazyDb.Value?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing LiteDB database");
                }
            }

            _databases.Clear();
            _activeFiles.Clear();
            _fileKeyTenantIndex.Clear();
            _pendingKeys.Clear();
            _pendingFileCounts.Clear();

            // 4. Dispose all tenant locks
            foreach (var semaphore in _tenantLocks.Values)
            {
                try
                {
                    semaphore?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error disposing tenant lock");
                }
            }
            _tenantLocks.Clear();

            // 5. Dispose write-behind resources
            _persistenceSignal.Dispose();
            _persistenceCts.Dispose();

            _logger.LogInformation("MetadataRepository disposed");
        }
    }
}
