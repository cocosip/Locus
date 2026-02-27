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

        // Per-tenant LiteDB databases (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<LiteDatabase>> _databases;

        // Per-tenant locks for concurrent file allocation
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;

        // --- Write-Behind: async LiteDB persistence (netstandard2.0 compatible) ---
        // Memory is always updated first. LiteDB writes are decoupled via this queue.
        // If LiteDB is unavailable, writes buffer in memory and are retried by the background loop.
        // On crash, in-flight writes are lost — physical files stay on disk as orphans and are
        // recovered by the cleanup service on next startup.
        private readonly ConcurrentQueue<PersistenceOperation> _persistenceQueue;
        private readonly SemaphoreSlim _persistenceSignal; // one permit per enqueued item
        private readonly Task _persistenceTask;
        private readonly CancellationTokenSource _persistenceCts;

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
            LiteDBOptions? liteDbOptions = null)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(metadataDirectory))
                throw new ArgumentException("Metadata directory cannot be empty", nameof(metadataDirectory));

            _metadataDirectory = metadataDirectory;
            _liteDbOptions = liteDbOptions ?? new LiteDBOptions();
            _activeFiles = new ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>>();
            _databases = new ConcurrentDictionary<string, Lazy<LiteDatabase>>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();

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

            // Start background persistence loop LAST — all fields are fully initialized at this point.
            // The task immediately blocks on _persistenceSignal.WaitAsync() and consumes no CPU
            // until EnqueuePersistence() is called. Dispose() cancels _persistenceCts and waits
            // for the task to drain remaining items before closing databases.
            _persistenceTask = Task.Run(() => RunPersistenceLoopAsync(_persistenceCts.Token));
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
                    // Database is corrupted during initialization - attempt automatic recovery
                    _logger.LogError(ex, "CORRUPTED METADATA DATABASE DETECTED for tenant {TenantId} during initialization. Attempting automatic recovery...", tid);

                    try
                    {
                        // Remove from cache to force rebuild
                        _databases.TryRemove(tid, out _);
                        _activeFiles.TryRemove(tid, out _);

                        // Rebuild database synchronously (we're in Lazy initialization)
                        var backupPath = BeginDatabaseRebuildAsync(tid, CancellationToken.None).GetAwaiter().GetResult();
                        FinishDatabaseRebuild(tid);

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
                foreach (var file in activeFiles)
                {
                    cache[file.FileKey] = file;
                }

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

            // 1. Update in-memory cache FIRST — always succeeds, immediately visible to readers
            var cache = _activeFiles.GetOrAdd(metadata.TenantId,
                _ => new ConcurrentDictionary<string, FileMetadata>());
            cache[metadata.FileKey] = metadata;

            _logger.LogDebug("Added/updated metadata for file: {FileKey}, Tenant: {TenantId}, Status: {Status}",
                metadata.FileKey, metadata.TenantId, metadata.Status);

            // 2. Queue LiteDB persistence — caller is not blocked; background loop drains the queue.
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

            // Update in-memory cache
            var cache = _activeFiles.GetOrAdd(metadata.TenantId,
                _ => new ConcurrentDictionary<string, FileMetadata>());
            cache[metadata.FileKey] = metadata;

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
            var removed = cache.TryRemove(fileKey, out _);

            if (removed)
            {
                // Queue LiteDB deletion — caller is not blocked by disk I/O
                EnqueuePersistence(new PersistenceOperation(tenantId, fileKey));

                _logger.LogDebug("Removed metadata for file: {FileKey}, Tenant: {TenantId}", fileKey, tenantId);
            }

            return Task.FromResult(removed);
        }

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

        /// <summary>
        /// Gets the next pending file for processing (atomic operation).
        /// Returns null if no files are available.
        /// Thread-safe: Uses per-tenant lock to prevent concurrent access.
        /// </summary>
        public async Task<FileMetadata?> GetNextPendingFileAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // Get or create tenant-specific lock
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));

            // Acquire lock to ensure atomic find-and-update
            await tenantLock.WaitAsync(ct);
            try
            {
                var cache = GetCache(tenantId);
                var now = DateTime.UtcNow;

                // Find the oldest pending file that is available for processing
                var file = cache.Values
                    .Where(m => m.Status == FileProcessingStatus.Pending
                             && (m.AvailableForProcessingAt == null || m.AvailableForProcessingAt <= now))
                    .OrderBy(m => m.CreatedAt)
                    .FirstOrDefault();

                if (file != null)
                {
                    // Update memory immediately (other threads polling under this same lock will
                    // now see the file as Processing and skip it).
                    file.Status = FileProcessingStatus.Processing;
                    file.ProcessingStartTime = DateTime.UtcNow;

                    // Write-Behind: LiteDB updated asynchronously. If the process crashes before
                    // the flush, the file's status in LiteDB is still Pending. On restart the
                    // cleanup service will pick it up again (correct behavior — no data loss).
                    EnqueuePersistence(new PersistenceOperation(file));

                    _logger.LogDebug("Allocated file for processing: {FileKey}, Tenant: {TenantId}", file.FileKey, tenantId);
                }

                return file;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Gets a batch of pending files for processing (atomic operation).
        /// Thread-safe: Updates LiteDB with rollback on failure.
        /// </summary>
        public Task<IEnumerable<FileMetadata>> GetNextPendingBatchAsync(
            string tenantId,
            int batchSize,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (batchSize <= 0)
                throw new ArgumentException("Batch size must be greater than zero", nameof(batchSize));

            var cache = GetCache(tenantId);
            var now = DateTime.UtcNow;
            var results = new List<FileMetadata>();

            // Find pending files
            var candidates = cache.Values
                .Where(m => m.Status == FileProcessingStatus.Pending
                         && (m.AvailableForProcessingAt == null || m.AvailableForProcessingAt <= now))
                .OrderBy(m => m.CreatedAt)
                .Take(batchSize)
                .ToList();

            if (candidates.Count == 0)
                return Task.FromResult<IEnumerable<FileMetadata>>(results);

            // Write-Behind: update memory immediately, persist async.
            // On crash, stuck Processing files are reset to Pending by the cleanup service.
            foreach (var file in candidates)
            {
                file.Status = FileProcessingStatus.Processing;
                file.ProcessingStartTime = DateTime.UtcNow;
                EnqueuePersistence(new PersistenceOperation(file));
                results.Add(file);
            }

            _logger.LogDebug("Allocated {Count} files for processing, Tenant: {TenantId}", results.Count, tenantId);

            return Task.FromResult<IEnumerable<FileMetadata>>(results);
        }

        /// <summary>
        /// Resets files that have been in Processing status for longer than the timeout.
        /// Thread-safe: Updates LiteDB with rollback on failure.
        /// </summary>
        public Task<int> ResetTimedOutFilesAsync(TimeSpan timeout, CancellationToken ct)
        {
            var cutoffTime = DateTime.UtcNow - timeout;
            var count = 0;

            foreach (var kvp in _activeFiles)
            {
                var tenantId = kvp.Key;
                var cache = kvp.Value;
                var db = GetDatabase(tenantId);
                var files = db.GetCollection<FileMetadata>("files");

                foreach (var file in cache.Values.ToList())
                {
                    if (file.Status == FileProcessingStatus.Processing
                        && file.ProcessingStartTime.HasValue
                        && file.ProcessingStartTime.Value < cutoffTime)
                    {
                        var processingDuration = DateTime.UtcNow - file.ProcessingStartTime.Value;

                        // Save original state for rollback
                        var originalStatus = file.Status;
                        var originalProcessingStartTime = file.ProcessingStartTime;
                        var originalAvailableForProcessingAt = file.AvailableForProcessingAt;

                        try
                        {
                            file.Status = FileProcessingStatus.Pending;
                            file.ProcessingStartTime = null;
                            file.AvailableForProcessingAt = null;
                            files.Update(file);
                            count++;

                            _logger.LogWarning("Reset timed-out file: {FileKey}, Tenant: {TenantId}, was processing for {Duration}",
                                file.FileKey, tenantId, processingDuration);
                        }
                        catch
                        {
                            // Rollback memory state on persistence failure
                            file.Status = originalStatus;
                            file.ProcessingStartTime = originalProcessingStartTime;
                            file.AvailableForProcessingAt = originalAvailableForProcessingAt;
                            throw;
                        }
                    }
                }
            }

            return Task.FromResult(count);
        }

        /// <summary>
        /// Finds file metadata by fileKey across all tenants without scanning every file.
        /// Iterates tenant dictionaries only (O(tenants)), then does a direct key lookup (O(1)).
        /// Use this instead of GetAllAsync().FirstOrDefault() when only the fileKey is known.
        /// </summary>
        public Task<FileMetadata?> GetByFileKeyAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            foreach (var tenantFiles in _activeFiles.Values)
            {
                if (tenantFiles.TryGetValue(fileKey, out var metadata))
                    return Task.FromResult<FileMetadata?>(metadata);
            }

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
            if (!_fileSystem.Directory.Exists(_metadataDirectory))
                return [];

            // Use the file system directly to get the database files
            var dbFiles = _fileSystem.Directory.GetFiles(_metadataDirectory, "*.db");
            var tenantIds = dbFiles
                .Select(f => _fileSystem.Path.GetFileNameWithoutExtension(f))
                .Where(name => !string.IsNullOrWhiteSpace(name)
                    && !name.Contains("-backup", StringComparison.OrdinalIgnoreCase)    // Filter LiteDB backup files: "tenant-001.db-backup-1"
                    && !name.Contains(".corrupted.", StringComparison.OrdinalIgnoreCase) // Filter corruption backups
                    && !name.EndsWith("-journal", StringComparison.OrdinalIgnoreCase))   // Filter LiteDB journal files
                .ToList();

            return await Task.FromResult(tenantIds);
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

                // Step 3: Clear in-memory cache
                _activeFiles.TryRemove(tenantId, out _);

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

        // Enqueues an operation and wakes the background persistence loop.
        // If the queue is full (LiteDB has been unavailable for a long time), the operation
        // is dropped with a warning — memory state is already correct, so data is not lost
        // within this process lifetime. On restart, the cleanup service reconciles disk vs metadata.
        private void EnqueuePersistence(PersistenceOperation op)
        {
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
                    _logger.LogWarning(
                        "Persistence queue is full ({Max} items). Dropping LiteDB upsert for {FileKey} (tenant {TenantId}). " +
                        "File is safe on disk; metadata will be lost if process restarts before LiteDB recovers.",
                        MaxPersistenceQueueSize, op.FileKey, op.TenantId);
                return;
            }

            _persistenceQueue.Enqueue(op);
            _persistenceSignal.Release(); // wake the background loop
        }

        // Background loop: waits for signals, dequeues, and writes to LiteDB.
        // If LiteDB throws, the error is logged and the item is discarded — the physical
        // file is already safe on disk and will be recovered by the cleanup service on restart.
        private async Task RunPersistenceLoopAsync(CancellationToken ct)
        {
            while (true)
            {
                try
                {
                    await _persistenceSignal.WaitAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    break; // shutdown requested
                }

                if (_persistenceQueue.TryDequeue(out var op))
                    ExecutePersistenceOperation(op);
            }

            // Drain remaining items enqueued before cancellation
            while (_persistenceQueue.TryDequeue(out var op))
                ExecutePersistenceOperation(op);
        }

        // Executes a single persistence operation against LiteDB.
        // Errors are logged but never propagated — LiteDB unavailability must never fail writes.
        private void ExecutePersistenceOperation(PersistenceOperation op)
        {
            try
            {
                var db = GetDatabase(op.TenantId);
                var files = db.GetCollection<FileMetadata>("files");

                if (op.IsDelete)
                {
                    files.Delete(op.FileKey);
                }
                else
                {
                    files.Upsert(op.Metadata!);
                }
            }
            catch (Exception ex)
            {
                if (op.IsDelete)
                    _logger.LogError(ex,
                        "LiteDB delete failed for file {FileKey} (tenant {TenantId}). " +
                        "Stale metadata record may remain; cleanup service will remove it on next run.",
                        op.FileKey, op.TenantId);
                else
                    _logger.LogError(ex,
                        "LiteDB upsert failed for file {FileKey} (tenant {TenantId}). " +
                        "File is safe on disk; metadata may be lost if process restarts before LiteDB recovers.",
                        op.FileKey, op.TenantId);
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
