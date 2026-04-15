using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Dapper;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Repository for file metadata storage using per-tenant SQLite with active-data in-memory caching.
    /// Keeps all non-deleted file states in memory, including Completed files awaiting background reaping.
    ///
    /// Write-Behind Architecture:
    /// - AddOrUpdateAsync and RemoveAsync update in-memory cache first, then enqueue SQLite persistence asynchronously.
    /// - If SQLite is unavailable, file writes continue uninterrupted. The physical file on disk is always safe.
    /// - If the process crashes before the background persistence loop flushes to SQLite, the in-memory metadata is lost.
    /// - On process restart, orphaned physical files (files on disk without metadata) are recovered by StorageCleanupService.RecoverOrphanedFilesAsync.
    ///
    /// Data Consistency Guarantees:
    /// - Physical files are ALWAYS written synchronously before metadata is updated.
    /// - Metadata in memory is immediately visible to all readers within the same process.
    /// - SQLite persistence is asynchronous and best-effort. If SQLite fails, the operation is logged and retried.
    /// - If the process crashes before SQLite persistence completes, the physical file becomes an "orphan" and will be
    ///   recovered on next startup as a Pending file for reprocessing.
    ///
    /// Failure Scenarios:
    /// 1. SQLite unavailable (disk full, locked, etc.):
    ///    - Physical file exists on disk, metadata in memory is correct.
    ///    - Persistence queue will retry until SQLite recovers.
    ///    - If process crashes before retry succeeds, file is recovered as orphan on restart.
    ///
    /// 2. Process crash during write:
    ///    - If crash occurs BEFORE physical write completes: No file on disk, no metadata. Clean state.
    ///    - If crash occurs AFTER physical write but BEFORE metadata update: Orphan file on disk, recovered on restart.
    ///    - If crash occurs AFTER metadata update but BEFORE SQLite flush: Metadata in memory lost, but physical file
    ///      exists. Recovered as orphan on restart (metadata will be rebuilt from physical file).
    ///
    /// 3. Persistence queue full:
    ///    - Operations are coalesced by file key, keeping only the latest state.
    ///    - Intermediate updates may be lost, but final state is preserved.
    ///
    /// Thread-safe for concurrent access.
    /// </summary>
    public class MetadataRepository : IDisposable, IQueueProjectionWritePathDiagnostics
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<MetadataRepository> _logger;
        private readonly string _metadataDirectory;
        private readonly SqliteOptions _sqliteOptions;

        // Per-tenant in-memory cache for all non-deleted file states.
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>> _activeFiles;

        // Global fileKey -> tenantId index for O(1) cross-tenant lookup by file key.
        private readonly ConcurrentDictionary<string, string> _fileKeyTenantIndex;
        private volatile string[] _cachedTenantDirectoryIds = Array.Empty<string>();
        private long _cachedTenantDirectoryIdsTicks;
        private static readonly long TenantDirectoryCacheTicks = Stopwatch.Frequency;
        private readonly StringComparer _pathComparer;

        // Per-tenant normalized physical path -> file key index for O(1) orphan-rebuild path checks.
        // This avoids rebuilding large per-run HashSet snapshots in StorageCleanupService.
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> _physicalPathIndex;

        // Per-tenant SQLite connections (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<SqliteConnection>> _databases;
        private readonly ConcurrentDictionary<string, object> _databaseLocks;

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
        private readonly ConcurrentDictionary<string, ConcurrentQueue<PendingQueueEntry>> _pendingKeys;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, long>> _pendingGenerations;
        private readonly ConcurrentDictionary<string, ConcurrentQueue<FileMetadata>> _prefetchedPendingByTenant;

        // Per-tenant delayed queue for Pending files that are not yet available for processing.
        // Ordered by AvailableForProcessingAt (min-heap) to promote ready items efficiently.
        private readonly ConcurrentDictionary<string, DelayedQueue> _delayedQueues;
        private readonly ConcurrentDictionary<string, object> _delayedQueueLocks;
        private long _delayedSequence;

        // Per-tenant count of actual Pending files (maintained in AddOrUpdateAsync / RemoveAsync).
        // Allows CompactPendingQueues to decide whether a queue is bloated in O(1) rather than
        // scanning the entire cache to count Pending entries.
        // Access via Interlocked only.
        private readonly ConcurrentDictionary<string, int> _pendingFileCounts;
        private long _pendingDequeuedAttempts;
        private long _pendingDequeuedStaleSkips;
        private int _pendingStaleCompactionCounter;

        // Per-tenant status indexes for cleanup hot paths.
        private readonly ConcurrentDictionary<string, StatusTimestampIndex> _processingByStartTime;
        private readonly ConcurrentDictionary<string, object> _processingByStartTimeLocks;
        private readonly ConcurrentDictionary<string, StatusTimestampIndex> _completedByCompletedAt;
        private readonly ConcurrentDictionary<string, object> _completedByCompletedAtLocks;
        private readonly ConcurrentDictionary<string, StatusTimestampIndex> _deleteRequestedByCompletedAt;
        private readonly ConcurrentDictionary<string, object> _deleteRequestedByCompletedAtLocks;
        private readonly ConcurrentDictionary<string, StatusTimestampIndex> _permanentlyFailedByLastFailedAt;
        private readonly ConcurrentDictionary<string, object> _permanentlyFailedByLastFailedAtLocks;
        private long _statusIndexSequence;
        private long _statusIndexStaleSkips;

        // --- Write-Behind: async SQLite persistence (netstandard2.0 compatible) ---
        // Memory is always updated first. SQLite writes are decoupled via this queue.
        // If SQLite is unavailable, writes buffer in memory and are retried by the background loop.
        // On crash, in-flight writes are lost -- physical files stay on disk as orphans and are
        // recovered by the cleanup service on next startup.
        private readonly Channel<PersistenceOperation> _persistenceChannel;
        private readonly ChannelWriter<PersistenceOperation> _persistenceWriter;
        private readonly ChannelReader<PersistenceOperation> _persistenceReader;
        private readonly Task _persistenceTask;
        private readonly CancellationTokenSource _persistenceCts;
        private readonly bool _enableBackgroundPersistence;
        private readonly int _maxPersistenceQueueSize;
        private readonly int _maxDrainBatchSize;
        private readonly int _persistenceQueueSoftMergeThreshold;
        private readonly int _startupLoadBatchSize;
        private readonly int _shutdownDrainTimeoutSeconds;
        private readonly int _persistenceIntervalSeconds;
        private long _observedProjectionWriteCount;
        private long _observedProjectionValidationCount;
        private long _observedProjectionValidationTicks;
        private long _observedProjectionCacheAndIndexCount;
        private long _observedProjectionCacheAndIndexTicks;
        private long _observedProjectionCacheMutationCount;
        private long _observedProjectionCacheMutationTicks;
        private long _observedProjectionPhysicalPathIndexCount;
        private long _observedProjectionPhysicalPathIndexTicks;
        private long _observedProjectionPendingQueueUpdateCount;
        private long _observedProjectionPendingQueueTicks;
        private long _observedProjectionStatusIndexUpdateCount;
        private long _observedProjectionStatusIndexTicks;
        private long _observedProjectionPersistenceEnqueueCount;
        private long _observedProjectionPersistenceEnqueueTicks;
        private readonly ConcurrentDictionary<string, PersistenceOperation> _coalescedPersistenceOps;
        private int _coalescedLogCounter;
        private int _persistenceChannelDepth;
        private int _persistenceChannelPeakDepth;
        private int _coalescedDepth;

        private volatile bool _disposed;

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

        private sealed class DelayedQueue
        {
            private readonly List<DelayedEntry> _heap = new List<DelayedEntry>();

            public int Count => _heap.Count;

            public void Enqueue(string fileKey, long generation, DateTime availableAtUtc, long sequence)
            {
                _heap.Add(new DelayedEntry(availableAtUtc, sequence, fileKey, generation));
                HeapifyUp(_heap.Count - 1);
            }

            public bool TryDequeueReady(DateTime nowUtc, out PendingQueueEntry entry)
            {
                if (_heap.Count == 0)
                {
                    entry = default(PendingQueueEntry);
                    return false;
                }

                var top = _heap[0];
                if (top.AvailableAtUtc > nowUtc)
                {
                    entry = default(PendingQueueEntry);
                    return false;
                }

                entry = new PendingQueueEntry(top.FileKey, top.Generation);
                RemoveRoot();
                return true;
            }

            private void RemoveRoot()
            {
                var lastIndex = _heap.Count - 1;
                if (lastIndex == 0)
                {
                    _heap.Clear();
                    return;
                }

                _heap[0] = _heap[lastIndex];
                _heap.RemoveAt(lastIndex);
                HeapifyDown(0);
            }

            private void HeapifyUp(int index)
            {
                while (index > 0)
                {
                    var parent = (index - 1) / 2;
                    if (IsHigherPriority(_heap[parent], _heap[index]))
                        break;

                    Swap(parent, index);
                    index = parent;
                }
            }

            private void HeapifyDown(int index)
            {
                var lastIndex = _heap.Count - 1;
                while (true)
                {
                    var left = index * 2 + 1;
                    var right = left + 1;
                    if (left > lastIndex)
                        return;

                    var smallest = left;
                    if (right <= lastIndex && IsHigherPriority(_heap[right], _heap[left]))
                        smallest = right;

                    if (IsHigherPriority(_heap[index], _heap[smallest]))
                        return;

                    Swap(index, smallest);
                    index = smallest;
                }
            }

            private void Swap(int a, int b)
            {
                var temp = _heap[a];
                _heap[a] = _heap[b];
                _heap[b] = temp;
            }

            private static bool IsHigherPriority(DelayedEntry left, DelayedEntry right)
            {
                if (left.AvailableAtUtc < right.AvailableAtUtc)
                    return true;
                if (left.AvailableAtUtc > right.AvailableAtUtc)
                    return false;
                return left.Sequence <= right.Sequence;
            }
        }

        private readonly struct PendingQueueEntry
        {
            public readonly string FileKey;
            public readonly long Generation;

            public PendingQueueEntry(string fileKey, long generation)
            {
                FileKey = fileKey;
                Generation = generation;
            }
        }

        private sealed class StatusTimestampIndex
        {
            private readonly List<StatusTimestampEntry> _heap = new List<StatusTimestampEntry>();

            public int Count => _heap.Count;

            public void Enqueue(string fileKey, DateTime timestampUtc, DateTime createdAtUtc, long sequence)
            {
                _heap.Add(new StatusTimestampEntry(timestampUtc, createdAtUtc, sequence, fileKey));
                HeapifyUp(_heap.Count - 1);
            }

            public bool TryPeek(out StatusTimestampEntry entry)
            {
                if (_heap.Count == 0)
                {
                    entry = default(StatusTimestampEntry);
                    return false;
                }

                entry = _heap[0];
                return true;
            }

            public bool TryDequeue(out StatusTimestampEntry entry)
            {
                if (!TryPeek(out entry))
                    return false;

                RemoveRoot();
                return true;
            }

            private void RemoveRoot()
            {
                var lastIndex = _heap.Count - 1;
                if (lastIndex == 0)
                {
                    _heap.Clear();
                    return;
                }

                _heap[0] = _heap[lastIndex];
                _heap.RemoveAt(lastIndex);
                HeapifyDown(0);
            }

            private void HeapifyUp(int index)
            {
                while (index > 0)
                {
                    var parent = (index - 1) / 2;
                    if (IsHigherPriority(_heap[parent], _heap[index]))
                        break;

                    Swap(parent, index);
                    index = parent;
                }
            }

            private void HeapifyDown(int index)
            {
                var lastIndex = _heap.Count - 1;
                while (true)
                {
                    var left = index * 2 + 1;
                    var right = left + 1;
                    if (left > lastIndex)
                        return;

                    var smallest = left;
                    if (right <= lastIndex && IsHigherPriority(_heap[right], _heap[left]))
                        smallest = right;

                    if (IsHigherPriority(_heap[index], _heap[smallest]))
                        return;

                    Swap(index, smallest);
                    index = smallest;
                }
            }

            private void Swap(int a, int b)
            {
                var temp = _heap[a];
                _heap[a] = _heap[b];
                _heap[b] = temp;
            }

            private static bool IsHigherPriority(StatusTimestampEntry left, StatusTimestampEntry right)
            {
                if (left.TimestampUtc < right.TimestampUtc)
                    return true;
                if (left.TimestampUtc > right.TimestampUtc)
                    return false;

                if (left.CreatedAtUtc < right.CreatedAtUtc)
                    return true;
                if (left.CreatedAtUtc > right.CreatedAtUtc)
                    return false;

                return left.Sequence <= right.Sequence;
            }
        }

        private readonly struct StatusTimestampEntry
        {
            public readonly DateTime TimestampUtc;
            public readonly DateTime CreatedAtUtc;
            public readonly long Sequence;
            public readonly string FileKey;

            public StatusTimestampEntry(DateTime timestampUtc, DateTime createdAtUtc, long sequence, string fileKey)
            {
                TimestampUtc = timestampUtc;
                CreatedAtUtc = createdAtUtc;
                Sequence = sequence;
                FileKey = fileKey;
            }
        }

        private readonly struct DelayedEntry
        {
            public readonly DateTime AvailableAtUtc;
            public readonly long Sequence;
            public readonly string FileKey;
            public readonly long Generation;

            public DelayedEntry(DateTime availableAtUtc, long sequence, string fileKey, long generation)
            {
                AvailableAtUtc = availableAtUtc;
                Sequence = sequence;
                FileKey = fileKey;
                Generation = generation;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepository"/> class.
        /// </summary>
        public MetadataRepository(
            IFileSystem fileSystem,
            ILogger<MetadataRepository> logger,
            string metadataDirectory,
            SqliteOptions? sqliteOptions = null,
            bool enableBackgroundPersistence = true,
            int maxDrainBatchSize = DefaultDrainBatchSize,
            int persistenceQueueSoftMergeThresholdPercent = DefaultPersistenceQueueSoftMergeThresholdPercent,
            int maxPersistenceQueueSize = DefaultMaxPersistenceQueueSize,
            int startupLoadBatchSize = DefaultStartupLoadBatchSize,
            int shutdownDrainTimeoutSeconds = DefaultShutdownDrainTimeoutSeconds,
            int persistenceIntervalSeconds = DefaultPersistenceIntervalSeconds)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(metadataDirectory))
                throw new ArgumentException("Metadata directory cannot be empty", nameof(metadataDirectory));

            if (maxDrainBatchSize <= 0)
                throw new ArgumentException("Drain batch size must be greater than zero", nameof(maxDrainBatchSize));

            if (persistenceQueueSoftMergeThresholdPercent <= 0 || persistenceQueueSoftMergeThresholdPercent > 100)
                throw new ArgumentException(
                    "Soft merge threshold percent must be between 1 and 100",
                    nameof(persistenceQueueSoftMergeThresholdPercent));

            if (maxPersistenceQueueSize <= 0)
                throw new ArgumentException("Max persistence queue size must be greater than zero", nameof(maxPersistenceQueueSize));
            if (startupLoadBatchSize <= 0)
                throw new ArgumentException("Startup load batch size must be greater than zero", nameof(startupLoadBatchSize));

            _metadataDirectory = metadataDirectory;
            _sqliteOptions = sqliteOptions ?? new SqliteOptions();
            _enableBackgroundPersistence = enableBackgroundPersistence;
            _maxPersistenceQueueSize = maxPersistenceQueueSize;
            _maxDrainBatchSize = Math.Min(maxDrainBatchSize, _maxPersistenceQueueSize);
            _persistenceQueueSoftMergeThreshold = (int)(_maxPersistenceQueueSize * (persistenceQueueSoftMergeThresholdPercent / 100.0));
            _startupLoadBatchSize = startupLoadBatchSize;
            _shutdownDrainTimeoutSeconds = shutdownDrainTimeoutSeconds > 0 ? shutdownDrainTimeoutSeconds : DefaultShutdownDrainTimeoutSeconds;
            _persistenceIntervalSeconds = persistenceIntervalSeconds > 0 ? persistenceIntervalSeconds : DefaultPersistenceIntervalSeconds;
            _activeFiles = new ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>>();
            _fileKeyTenantIndex = new ConcurrentDictionary<string, string>(StringComparer.Ordinal);
            _pathComparer = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal;
            _physicalPathIndex = new ConcurrentDictionary<string, ConcurrentDictionary<string, string>>();
            _databases = new ConcurrentDictionary<string, Lazy<SqliteConnection>>();
            _databaseLocks = new ConcurrentDictionary<string, object>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _pendingKeys = new ConcurrentDictionary<string, ConcurrentQueue<PendingQueueEntry>>();
            _pendingGenerations = new ConcurrentDictionary<string, ConcurrentDictionary<string, long>>();
            _prefetchedPendingByTenant = new ConcurrentDictionary<string, ConcurrentQueue<FileMetadata>>();
            _pendingFileCounts = new ConcurrentDictionary<string, int>();
            _processingByStartTime = new ConcurrentDictionary<string, StatusTimestampIndex>();
            _processingByStartTimeLocks = new ConcurrentDictionary<string, object>();
            _completedByCompletedAt = new ConcurrentDictionary<string, StatusTimestampIndex>();
            _completedByCompletedAtLocks = new ConcurrentDictionary<string, object>();
            _deleteRequestedByCompletedAt = new ConcurrentDictionary<string, StatusTimestampIndex>();
            _deleteRequestedByCompletedAtLocks = new ConcurrentDictionary<string, object>();
            _permanentlyFailedByLastFailedAt = new ConcurrentDictionary<string, StatusTimestampIndex>();
            _permanentlyFailedByLastFailedAtLocks = new ConcurrentDictionary<string, object>();
            _delayedQueues = new ConcurrentDictionary<string, DelayedQueue>();
            _delayedQueueLocks = new ConcurrentDictionary<string, object>();

            // Write-behind channel (fields initialized here, task started LAST)
            _persistenceChannel = Channel.CreateBounded<PersistenceOperation>(new BoundedChannelOptions(_maxPersistenceQueueSize)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            });
            _persistenceWriter = _persistenceChannel.Writer;
            _persistenceReader = _persistenceChannel.Reader;
            _persistenceCts = new CancellationTokenSource();
            _coalescedPersistenceOps = new ConcurrentDictionary<string, PersistenceOperation>(StringComparer.Ordinal);

            // Ensure metadata directory exists before starting the background task,
            // so that if CreateDirectory throws, the task is never started and cannot be orphaned.
            if (!_fileSystem.Directory.Exists(_metadataDirectory))
            {
                _fileSystem.Directory.CreateDirectory(_metadataDirectory);
            }

            _logger.LogInformation(
                "MetadataRepository initialized at {Directory} with SQLite write-behind: JournalMode={JournalMode}, SynchronousMode={SynchronousMode}, BusyTimeout={BusyTimeout}ms, DrainBatch={DrainBatch}, SoftMergeThreshold={SoftMergeThreshold}",
                _metadataDirectory,
                _sqliteOptions.JournalMode,
                _sqliteOptions.SynchronousMode,
                _sqliteOptions.BusyTimeoutMs,
                _maxDrainBatchSize,
                _persistenceQueueSoftMergeThreshold);

            if (_enableBackgroundPersistence)
            {
                // Start background persistence loop LAST -- all fields are fully initialized at this point.
                // The task blocks on ChannelReader.WaitToReadAsync until items are available.
                // Dispose() cancels _persistenceCts and waits
                // for the task to drain remaining items before closing connections.
                _persistenceTask = Task.Run(() => RunPersistenceLoopAsync(_persistenceCts.Token));
            }
            else
            {
                _persistenceTask = Task.CompletedTask;
                _logger.LogInformation("MetadataRepository write-behind background loop is disabled; persistence is synchronous.");
            }
        }

        private const string FilesDdl = @"
CREATE TABLE IF NOT EXISTS files (
    file_key TEXT PRIMARY KEY NOT NULL,
    tenant_id TEXT NOT NULL,
    volume_id TEXT NOT NULL,
    physical_path TEXT NOT NULL,
    directory_path TEXT NOT NULL,
    file_size INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    status INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_failed_at TEXT,
    last_error TEXT,
    processing_start_time TEXT,
    completed_at TEXT,
    delete_succeeded_at TEXT,
    dead_lettered_at TEXT,
    available_for_processing_at TEXT,
    original_file_name TEXT,
    file_extension TEXT,
    metadata_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_status ON files(status);
CREATE INDEX IF NOT EXISTS idx_files_created_at ON files(created_at);
CREATE INDEX IF NOT EXISTS idx_files_available_at ON files(available_for_processing_at);
CREATE INDEX IF NOT EXISTS idx_files_completed_at ON files(completed_at);";

        /// <summary>
        /// Gets or creates a SQLite connection for a tenant.
        /// Auto-recovery: Rebuilds corrupted database on initialization failure.
        /// </summary>
        private SqliteConnection GetDatabase(string tenantId)
        {
            // Use Lazy<SqliteConnection> to ensure thread-safe initialization.
            // This prevents multiple threads from simultaneously creating the connection instance.
            var lazyConn = _databases.GetOrAdd(tenantId, tid => new Lazy<SqliteConnection>(() =>
            {
                try
                {
                    // Ensure tenant subdirectory exists (thread-safe in case of concurrent access)
                    var tenantDir = _fileSystem.Path.Combine(_metadataDirectory, tid);
                    if (!_fileSystem.Directory.Exists(tenantDir))
                        _fileSystem.Directory.CreateDirectory(tenantDir);

                    var dbPath = _fileSystem.Path.Combine(tenantDir, "metadata.db");
                    var conn = OpenAndInitializeConnection(dbPath);

                    _logger.LogDebug("Created/opened SQLite database for tenant {TenantId} at {Path}", tid, dbPath);

                    // Load active files into memory on first access
                    LoadActiveFilesForTenant(tid, conn);

                    return conn;
                }
                catch (Exception ex) when (IsRecoverableDatabaseException(ex))
                {
                    // Database is corrupted during initialization - attempt automatic recovery.
                    // IMPORTANT: Do NOT call BeginDatabaseRebuildAsync here - it acquires _tenantLocks[tid],
                    // which another thread may already hold while waiting for this Lazy to complete,
                    // causing a deadlock (Lazy lock -> tenant lock cycle).
                    // Instead call the lock-free internal helper directly.
                    _logger.LogError(ex, "CORRUPTED METADATA DATABASE DETECTED for tenant {TenantId} during initialization. Attempting automatic recovery...", tid);

                    try
                    {
                        // IMPORTANT: do NOT remove _databases[tid] from inside this Lazy factory.
                        // Doing so would allow a concurrent caller to race in and install a second
                        // Lazy for the same tenant while this factory is still rebuilding the file.
                        // Clear only the tenant-specific in-memory indexes; the current Lazy instance
                        // will remain the single publisher and will return the recovered connection.
                        if (_activeFiles.TryRemove(tid, out var staleTenantCache))
                            RemoveTenantFromGlobalIndex(staleTenantCache);
                        _physicalPathIndex.TryRemove(tid, out _);

                        // Rebuild database without acquiring the tenant lock (we may be called
                        // from within the Lazy factory which is already locked via ExecutionAndPublication).
                        var backupPath = RebuildDatabaseFileNoLock(tid);

                        _logger.LogWarning("Metadata database rebuilt for tenant {TenantId} during initialization. Backup: {BackupPath}", tid, backupPath ?? "N/A");

                        // Retry initialization
                        var dbPath = _fileSystem.Path.Combine(_metadataDirectory, tid, "metadata.db");
                        var conn = OpenAndInitializeConnection(dbPath);

                        _logger.LogInformation("Successfully recovered and initialized metadata database for tenant {TenantId}", tid);

                        return conn;
                    }
                    catch (Exception recoveryEx)
                    {
                        _logger.LogError(recoveryEx, "Failed to recover corrupted metadata database for tenant {TenantId} during initialization", tid);
                        throw;
                    }
                }
            }, LazyThreadSafetyMode.ExecutionAndPublication));

            return lazyConn.Value;
        }

        private object GetDatabaseLock(string tenantId)
        {
            return _databaseLocks.GetOrAdd(tenantId, _ => new object());
        }

        private SqliteConnection OpenAndInitializeConnection(string dbPath)
        {
            var connStr = _sqliteOptions.BuildConnectionString(dbPath);
            var conn = new SqliteConnection(connStr);
            conn.Open();

            // Apply PRAGMAs
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = _sqliteOptions.BuildPragmaSql();
                cmd.ExecuteNonQuery();
            }

            // Create schema (idempotent)
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = FilesDdl;
                cmd.ExecuteNonQuery();
            }

            EnsureColumnExists(conn, "files", "completed_at", "TEXT");
            EnsureColumnExists(conn, "files", "delete_succeeded_at", "TEXT");
            EnsureColumnExists(conn, "files", "dead_lettered_at", "TEXT");

            return conn;
        }

        /// <summary>
        /// Loads active files into memory for a tenant.
        /// </summary>
        private void LoadActiveFilesForTenant(string tenantId, SqliteConnection conn)
        {
            var cache = _activeFiles.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, FileMetadata>());
            var pendingQueue = _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<PendingQueueEntry>());

            var nowUtc = DateTime.UtcNow;
            var pendingCount = 0;
            var totalLoaded = 0;
            var batchCount = 0;

            // Process non-pending statuses first; pending files are loaded in CreatedAt order afterwards
            // to rebuild FIFO scheduling without materializing all active records into one large list.
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.Processing, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.Completed, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.DeleteRequested, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.DeleteSucceeded, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.Failed, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.PermanentlyFailed, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.DeadLettered, orderByCreatedAt: false,
                nowUtc, ref pendingCount, ref batchCount);
            totalLoaded += LoadActiveFilesByStatusInBatches(
                tenantId, conn, cache, pendingQueue,
                FileProcessingStatus.Pending, orderByCreatedAt: true,
                nowUtc, ref pendingCount, ref batchCount);

            if (totalLoaded == 0)
                return;

            // Seed the O(1) pending count so CompactPendingQueues can check bloat without scanning.
            _pendingFileCounts[tenantId] = pendingCount;

            _logger.LogInformation(
                "Loaded {Count} active files for tenant {TenantId} into memory in {BatchCount} startup batch(es) (batchSize={BatchSize})",
                totalLoaded, tenantId, batchCount, _startupLoadBatchSize);
        }

        private int LoadActiveFilesByStatusInBatches(
            string tenantId,
            SqliteConnection conn,
            ConcurrentDictionary<string, FileMetadata> cache,
            ConcurrentQueue<PendingQueueEntry> pendingQueue,
            FileProcessingStatus status,
            bool orderByCreatedAt,
            DateTime nowUtc,
            ref int pendingCount,
            ref int batchCount)
        {
            var orderClause = orderByCreatedAt ? "ORDER BY created_at ASC" : string.Empty;
            var sql = $"SELECT * FROM files WHERE status = @status {orderClause}";

            var loaded = 0;
            var batch = new List<FileMetadata>(_startupLoadBatchSize);

            // Stream rows via Dapper buffered=false to avoid loading all rows into one list.
            var rows = conn.Query<FileMetadataRow>(sql, new { status = (int)status }, buffered: false);
            foreach (var row in rows)
            {
                batch.Add(row.ToFileMetadata());
                if (batch.Count < _startupLoadBatchSize)
                    continue;

                loaded += ApplyStartupLoadBatch(tenantId, cache, pendingQueue, batch, nowUtc, ref pendingCount);
                batch.Clear();
                batchCount++;
            }

            if (batch.Count > 0)
            {
                loaded += ApplyStartupLoadBatch(tenantId, cache, pendingQueue, batch, nowUtc, ref pendingCount);
                batchCount++;
            }

            return loaded;
        }

        private static void EnsureColumnExists(
            SqliteConnection conn,
            string tableName,
            string columnName,
            string columnDefinition)
        {
            using (var probe = conn.CreateCommand())
            {
                probe.CommandText = $"PRAGMA table_info({tableName});";
                using (var reader = probe.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (string.Equals(reader.GetString(1), columnName, StringComparison.OrdinalIgnoreCase))
                            return;
                    }
                }
            }

            using (var alter = conn.CreateCommand())
            {
                alter.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {columnName} {columnDefinition};";
                alter.ExecuteNonQuery();
            }
        }

        private int ApplyStartupLoadBatch(
            string tenantId,
            ConcurrentDictionary<string, FileMetadata> cache,
            ConcurrentQueue<PendingQueueEntry> pendingQueue,
            List<FileMetadata> batch,
            DateTime nowUtc,
            ref int pendingCount)
        {
            foreach (var file in batch)
            {
                cache[file.FileKey] = file;
                _fileKeyTenantIndex[file.FileKey] = tenantId;
                IndexPhysicalPathCandidate(tenantId, file.FileKey, previous: null, file);
                IndexStatusCandidate(null, file);

                if (file.Status != FileProcessingStatus.Pending)
                    continue;

                var generation = GetOrCreatePendingGeneration(tenantId, file.FileKey);
                var entry = new PendingQueueEntry(file.FileKey, generation);
                if (file.AvailableForProcessingAt.HasValue && file.AvailableForProcessingAt.Value > nowUtc)
                    EnqueueDelayed(tenantId, entry, file.AvailableForProcessingAt.Value);
                else
                    pendingQueue.Enqueue(entry);

                pendingCount++;
            }

            return batch.Count;
        }

        /// <summary>
        /// Gets the in-memory cache for a tenant.
        /// </summary>
        private ConcurrentDictionary<string, FileMetadata> GetCache(string tenantId)
        {
            // Ensure connection is initialized (which also loads active files)
            GetDatabase(tenantId);
            return _activeFiles.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, FileMetadata>());
        }

        /// <summary>
        /// Adds or updates file metadata.
        /// Write-Behind: updates in-memory cache immediately (never fails from caller's perspective),
        /// then enqueues SQLite persistence asynchronously via background loop.
        /// If SQLite is unavailable, the write is queued and retried; the file on disk remains safe.
        /// On process crash before flush, the physical file becomes an orphan recovered by cleanup service.
        /// </summary>
        /// <remarks>
        /// For maintenance operations (database rebuild) that require immediate SQLite persistence,
        /// use <see cref="AddOrUpdateDirectAsync"/> instead.
        /// </remarks>
        public Task AddOrUpdateAsync(FileMetadata metadata, CancellationToken ct = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MetadataRepository));

            var validationStarted = Stopwatch.GetTimestamp();
            ValidateUpsertMetadata(metadata);
            RecordProjectionPhase(
                ref _observedProjectionValidationCount,
                ref _observedProjectionValidationTicks,
                Stopwatch.GetTimestamp() - validationStarted);

            // 1. Update in-memory cache FIRST -- always succeeds, immediately visible to readers.
            UpdateCacheAndIndices(metadata, observeProjectionWritePath: true);

            _logger.LogDebug("Added/updated metadata for file: {FileKey}, Tenant: {TenantId}, Status: {Status}",
                metadata.FileKey, metadata.TenantId, metadata.Status);

            // 2. Queue SQLite persistence -- caller is not blocked; background loop drains the queue.
            var persistenceEnqueueStarted = Stopwatch.GetTimestamp();
            EnqueuePersistence(new PersistenceOperation(metadata));
            RecordProjectionPhase(
                ref _observedProjectionPersistenceEnqueueCount,
                ref _observedProjectionPersistenceEnqueueTicks,
                Stopwatch.GetTimestamp() - persistenceEnqueueStarted);
            Interlocked.Increment(ref _observedProjectionWriteCount);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Begins a tenant-scoped direct projection batch that updates memory immediately
        /// and flushes staged SQLite mutations in one durable transaction.
        /// </summary>
        internal IQueueProjectionBatch BeginProjectionBatch(string tenantId)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MetadataRepository));

            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            return new DirectProjectionBatch(this, tenantId);
        }

        /// <summary>
        /// Adds or updates file metadata with immediate synchronous SQLite write.
        /// Bypasses the Write-Behind queue -- use only for maintenance operations such as
        /// database rebuild where the caller needs the data persisted before returning.
        /// </summary>
        internal Task AddOrUpdateDirectAsync(FileMetadata metadata, CancellationToken ct = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MetadataRepository));

            ValidateUpsertMetadata(metadata);

            // Update in-memory cache (same logic as AddOrUpdateAsync).
            UpdateCacheAndIndices(metadata);

            // Write directly to SQLite (synchronous -- required for rebuild correctness)
            ExecuteDirectBatch(metadata.TenantId, new List<PersistenceOperation> { new PersistenceOperation(metadata) });

            return Task.CompletedTask;
        }

        private static void ValidateUpsertMetadata(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.FileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(metadata));
        }

        /// <summary>
        /// Applies a metadata upsert to all in-memory structures shared between
        /// <see cref="AddOrUpdateAsync"/> and <see cref="AddOrUpdateDirectAsync"/>.
        /// <list type="number">
        ///   <item>Updates the per-tenant active-file cache and the cross-tenant file-key index.</item>
        ///   <item>Maintains the O(1) pending-file count used by <c>CompactPendingQueues</c>.</item>
        ///   <item>Enqueues the file as a pending candidate when it transitions into Pending status.</item>
        ///   <item>Updates the status-timestamp index for timed-out / permanently-failed queries.</item>
        /// </list>
        /// </summary>
        private void UpdateCacheAndIndices(FileMetadata metadata, bool observeProjectionWritePath = false)
        {
            var cacheAndIndexStarted = observeProjectionWritePath
                ? Stopwatch.GetTimestamp()
                : 0;
            var cache = _activeFiles.GetOrAdd(metadata.TenantId,
                _ => new ConcurrentDictionary<string, FileMetadata>());

            var cacheMutationStarted = observeProjectionWritePath
                ? Stopwatch.GetTimestamp()
                : 0;
            cache.TryGetValue(metadata.FileKey, out var previous);
            cache[metadata.FileKey] = metadata;
            _fileKeyTenantIndex[metadata.FileKey] = metadata.TenantId;
            if (observeProjectionWritePath)
            {
                RecordProjectionPhase(
                    ref _observedProjectionCacheMutationCount,
                    ref _observedProjectionCacheMutationTicks,
                    Stopwatch.GetTimestamp() - cacheMutationStarted);
            }

            var physicalPathIndexStarted = observeProjectionWritePath
                ? Stopwatch.GetTimestamp()
                : 0;
            IndexPhysicalPathCandidate(metadata.TenantId, metadata.FileKey, previous, metadata);
            if (observeProjectionWritePath)
            {
                RecordProjectionPhase(
                    ref _observedProjectionPhysicalPathIndexCount,
                    ref _observedProjectionPhysicalPathIndexTicks,
                    Stopwatch.GetTimestamp() - physicalPathIndexStarted);
            }

            // Maintain _pendingFileCounts: +1 when transitioning INTO Pending, -1 when leaving.
            // Keeps CompactPendingQueues O(1) instead of O(N).
            bool wasP = previous?.Status == FileProcessingStatus.Pending;
            bool isP  = metadata.Status  == FileProcessingStatus.Pending;
            var pendingQueueStarted = observeProjectionWritePath
                ? Stopwatch.GetTimestamp()
                : 0;
            if (!wasP && isP)
                _pendingFileCounts.AddOrUpdate(metadata.TenantId, 1, (_, c) => c + 1);
            else if (wasP && !isP)
                _pendingFileCounts.AddOrUpdate(metadata.TenantId, 0, (_, c) => Math.Max(0, c - 1));

            if (wasP && !isP)
                InvalidatePendingGeneration(metadata.TenantId, metadata.FileKey);

            // Enqueue file key when it becomes Pending so the allocator can find it in O(1).
            // Non-Pending transitions are not explicitly removed from the queue; stale entries
            // are validated and skipped when GetNextPendingFileAsync dequeues them.
            if (isP)
                EnqueuePendingCandidate(metadata, refreshGeneration: wasP);
            if (observeProjectionWritePath)
            {
                RecordProjectionPhase(
                    ref _observedProjectionPendingQueueUpdateCount,
                    ref _observedProjectionPendingQueueTicks,
                    Stopwatch.GetTimestamp() - pendingQueueStarted);
            }

            var statusIndexStarted = observeProjectionWritePath
                ? Stopwatch.GetTimestamp()
                : 0;
            IndexStatusCandidate(previous, metadata);
            if (observeProjectionWritePath)
            {
                RecordProjectionPhase(
                    ref _observedProjectionStatusIndexUpdateCount,
                    ref _observedProjectionStatusIndexTicks,
                    Stopwatch.GetTimestamp() - statusIndexStarted);
                RecordProjectionPhase(
                    ref _observedProjectionCacheAndIndexCount,
                    ref _observedProjectionCacheAndIndexTicks,
                    Stopwatch.GetTimestamp() - cacheAndIndexStarted);
            }
        }

        /// <summary>
        /// Gets file metadata by file key.
        /// All queries hit memory first (microseconds).
        /// </summary>
        public Task<FileMetadata?> GetAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var cache = GetCache(tenantId);
            cache.TryGetValue(fileKey, out var metadata);
            return Task.FromResult<FileMetadata?>(metadata?.Clone());
        }

        /// <summary>
        /// Removes file metadata by file key.
        /// Used when file processing is completed successfully.
        /// </summary>
        public Task<bool> RemoveAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MetadataRepository));

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
                RemovePhysicalPathCandidate(tenantId, fileKey, removedMetadata?.PhysicalPath);

                // Maintain _pendingFileCounts: if the removed file was Pending, decrement.
                if (removedMetadata?.Status == FileProcessingStatus.Pending)
                    _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));

                InvalidatePendingGeneration(tenantId, fileKey);

                // No explicit removal from _pendingKeys: ConcurrentQueue does not support
                // O(1) keyed removal. The stale entry (if any) will be skipped by the
                // dequeue-and-validate loop in GetNextPendingFileAsync.

                // Queue SQLite deletion -- caller is not blocked by disk I/O
                EnqueuePersistence(new PersistenceOperation(tenantId, fileKey));

                _logger.LogDebug("Removed metadata for file: {FileKey}, Tenant: {TenantId}", fileKey, tenantId);
            }

            return Task.FromResult(removed);
        }

        /// <summary>
        /// Removes file metadata by file key with immediate synchronous SQLite deletion.
        /// </summary>
        internal Task<bool> RemoveDirectAsync(string tenantId, string fileKey, CancellationToken ct = default)
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(MetadataRepository));

            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (TryRemoveFromCacheAndIndices(tenantId, fileKey, out _))
            {
                ExecuteDirectBatch(tenantId, new List<PersistenceOperation> { new PersistenceOperation(tenantId, fileKey) });

                _logger.LogDebug("Directly removed metadata for file: {FileKey}, Tenant: {TenantId}", fileKey, tenantId);
                return Task.FromResult(true);
            }

            return Task.FromResult(false);
        }

        private bool TryRemoveFromCacheAndIndices(string tenantId, string fileKey, out FileMetadata? removedMetadata)
        {
            var cache = GetCache(tenantId);
            var removed = cache.TryRemove(fileKey, out removedMetadata);
            if (!removed)
                return false;

            _fileKeyTenantIndex.TryRemove(fileKey, out _);
            RemovePhysicalPathCandidate(tenantId, fileKey, removedMetadata?.PhysicalPath);

            if (removedMetadata?.Status == FileProcessingStatus.Pending)
                _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));

            InvalidatePendingGeneration(tenantId, fileKey);
            return true;
        }

        private sealed class DirectProjectionBatch : IQueueProjectionBatch
        {
            private readonly MetadataRepository _repository;
            private readonly string _tenantId;
            private readonly List<PersistenceOperation> _operations;
            private bool _flushed;

            public DirectProjectionBatch(MetadataRepository repository, string tenantId)
            {
                _repository = repository;
                _tenantId = tenantId;
                _operations = new List<PersistenceOperation>();
            }

            public string TenantId => _tenantId;

            public Task UpsertProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default)
            {
                ct.ThrowIfCancellationRequested();

                if (_repository._disposed)
                    throw new ObjectDisposedException(nameof(MetadataRepository));

                if (_flushed)
                    throw new InvalidOperationException("Projection batch has already been flushed.");

                ValidateUpsertMetadata(metadata);

                if (!string.Equals(metadata.TenantId, _tenantId, StringComparison.Ordinal))
                    throw new ArgumentException("Metadata tenant does not match the active projection batch.", nameof(metadata));

                _repository.UpdateCacheAndIndices(metadata);
                _operations.Add(new PersistenceOperation(metadata));
                return Task.CompletedTask;
            }

            public Task<bool> RemoveProjectedFileAsync(string fileKey, CancellationToken ct = default)
            {
                ct.ThrowIfCancellationRequested();

                if (_repository._disposed)
                    throw new ObjectDisposedException(nameof(MetadataRepository));

                if (_flushed)
                    throw new InvalidOperationException("Projection batch has already been flushed.");

                if (string.IsNullOrWhiteSpace(fileKey))
                    throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

                var removed = _repository.TryRemoveFromCacheAndIndices(_tenantId, fileKey, out _);
                if (removed)
                    _operations.Add(new PersistenceOperation(_tenantId, fileKey));

                return Task.FromResult(removed);
            }

            public Task FlushAsync(CancellationToken ct = default)
            {
                ct.ThrowIfCancellationRequested();

                if (_flushed)
                    return Task.CompletedTask;

                _flushed = true;
                _repository.ExecuteDirectBatch(_tenantId, _operations);
                return Task.CompletedTask;
            }
        }

        /// <summary>
        /// Resets a timed-out Processing file back to Pending if it still matches the expected state.
        /// Returns false when the record was removed or changed concurrently.
        /// </summary>
        public async Task<bool> TryResetTimedOutFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            DateTime availableForProcessingAtUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return false;

                if (current.Status != FileProcessingStatus.Processing
                    || !current.ProcessingStartTime.HasValue
                    || current.ProcessingStartTime.Value != expectedProcessingStartTimeUtc)
                {
                    return false;
                }

                var updated = current.Clone();
                updated.Status = FileProcessingStatus.Pending;
                updated.ProcessingStartTime = null;
                updated.AvailableForProcessingAt = availableForProcessingAtUtc;

                if (!cache.TryUpdate(fileKey, updated, current))
                    return false;

                _pendingFileCounts.AddOrUpdate(tenantId, 1, (_, c) => c + 1);
                IndexStatusCandidate(current, updated);
                EnqueuePersistence(new PersistenceOperation(updated));
                EnqueuePendingCandidate(updated, refreshGeneration: false);
                return true;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Removes a PermanentlyFailed file if it still matches the expected failure timestamp.
        /// Returns false when the record was removed or changed concurrently.
        /// </summary>
        public async Task<bool> TryRemovePermanentlyFailedFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return false;

                if (current.Status != FileProcessingStatus.PermanentlyFailed
                    || !current.LastFailedAt.HasValue
                    || current.LastFailedAt.Value != expectedLastFailedAtUtc)
                {
                    return false;
                }

                var removed = ((ICollection<KeyValuePair<string, FileMetadata>>)cache)
                    .Remove(new KeyValuePair<string, FileMetadata>(fileKey, current));
                if (!removed)
                    return false;

                _fileKeyTenantIndex.TryRemove(fileKey, out _);
                RemovePhysicalPathCandidate(tenantId, fileKey, current.PhysicalPath);

                // PermanentlyFailed files are never in the Pending queue, so _pendingFileCounts
                // does not need adjustment here. The guard is intentionally omitted.
                InvalidatePendingGeneration(tenantId, fileKey);
                EnqueuePersistence(new PersistenceOperation(tenantId, fileKey));
                _logger.LogDebug("Conditionally removed permanently failed metadata for file: {FileKey}, Tenant: {TenantId}", fileKey, tenantId);
                return true;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Marks a PermanentlyFailed file as DeleteSucceeded so journal-driven cleanup can hand off
        /// final quota and metadata removal to the projection worker without reprocessing the same file.
        /// Returns false when the record was removed or changed concurrently.
        /// </summary>
        public async Task<bool> TryMarkPermanentlyFailedDeleteSucceededAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            DateTime deleteSucceededAtUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return false;

                if (current.Status != FileProcessingStatus.PermanentlyFailed
                    || !current.LastFailedAt.HasValue
                    || current.LastFailedAt.Value != expectedLastFailedAtUtc)
                {
                    return false;
                }

                if (current.DeleteSucceededAt.HasValue && current.DeleteSucceededAt.Value >= deleteSucceededAtUtc)
                    return true;

                var updated = current.Clone();
                updated.Status = FileProcessingStatus.DeleteSucceeded;
                updated.CompletedAt = updated.CompletedAt ?? deleteSucceededAtUtc;
                updated.DeleteSucceededAt = deleteSucceededAtUtc;

                if (!cache.TryUpdate(fileKey, updated, current))
                    return false;

                IndexStatusCandidate(current, updated);

                lock (GetDatabaseLock(tenantId))
                {
                    var conn = GetDatabase(tenantId);
                    UpsertFile(conn, updated);
                }

                return true;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Marks a PermanentlyFailed file as DeadLettered if it still matches the expected failure timestamp.
        /// Returns false when the record was removed or changed concurrently.
        /// </summary>
        public async Task<bool> TryMarkPermanentlyFailedDeadLetteredAsync(
            string tenantId,
            string fileKey,
            DateTime expectedLastFailedAtUtc,
            DateTime deadLetteredAtUtc,
            string deadLetterPhysicalPath,
            string? volumeId,
            bool projectionApplied,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (string.IsNullOrWhiteSpace(deadLetterPhysicalPath))
                throw new ArgumentException("Dead-letter physical path cannot be empty", nameof(deadLetterPhysicalPath));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return false;

                if (current.Status != FileProcessingStatus.PermanentlyFailed
                    || !current.LastFailedAt.HasValue
                    || current.LastFailedAt.Value != expectedLastFailedAtUtc)
                {
                    return false;
                }

                var updated = current.Clone();
                updated.Status = FileProcessingStatus.DeadLettered;
                updated.ProcessingStartTime = null;
                updated.CompletedAt = null;
                updated.DeleteSucceededAt = null;
                updated.DeadLetteredAt = deadLetteredAtUtc;
                updated.AvailableForProcessingAt = null;
                updated.PhysicalPath = deadLetterPhysicalPath;
                if (!string.IsNullOrWhiteSpace(volumeId))
                    updated.VolumeId = volumeId!;

                if (projectionApplied)
                    QueueProjectionMetadataState.MarkDeadLetterProjectionApplied(updated);
                else
                    QueueProjectionMetadataState.MarkDeadLetterProjectionPending(updated);

                if (!cache.TryUpdate(fileKey, updated, current))
                    return false;

                IndexStatusCandidate(current, updated);

                lock (GetDatabaseLock(tenantId))
                {
                    var conn = GetDatabase(tenantId);
                    UpsertFile(conn, updated);
                }

                return true;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Removes a Completed file if it still matches the expected completion timestamp.
        /// Returns false when the record was removed or changed concurrently.
        /// </summary>
        public async Task<bool> TryRemoveCompletedFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedCompletedAtUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return false;

                if ((current.Status != FileProcessingStatus.Completed
                    && current.Status != FileProcessingStatus.DeleteRequested)
                    || !current.CompletedAt.HasValue
                    || current.CompletedAt.Value != expectedCompletedAtUtc)
                {
                    return false;
                }

                var removed = ((ICollection<KeyValuePair<string, FileMetadata>>)cache)
                    .Remove(new KeyValuePair<string, FileMetadata>(fileKey, current));
                if (!removed)
                    return false;

                _fileKeyTenantIndex.TryRemove(fileKey, out _);
                RemovePhysicalPathCandidate(tenantId, fileKey, current.PhysicalPath);
                InvalidatePendingGeneration(tenantId, fileKey);
                EnqueuePersistence(new PersistenceOperation(tenantId, fileKey));
                _logger.LogDebug("Conditionally removed completed metadata for file: {FileKey}, Tenant: {TenantId}", fileKey, tenantId);
                return true;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Marks a Completed file as physically deleted so background cleanup does not append
        /// duplicate DeleteSucceeded events while projection catch-up is pending.
        /// Returns false when the record was removed or changed concurrently.
        /// </summary>
        internal async Task<bool> TryMarkDeleteSucceededAsync(
            string tenantId,
            string fileKey,
            DateTime expectedCompletedAtUtc,
            DateTime deleteSucceededAtUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return false;

                if ((current.Status != FileProcessingStatus.Completed
                    && current.Status != FileProcessingStatus.DeleteRequested)
                    || !current.CompletedAt.HasValue
                    || current.CompletedAt.Value != expectedCompletedAtUtc)
                {
                    return false;
                }

                if (current.DeleteSucceededAt.HasValue && current.DeleteSucceededAt.Value >= deleteSucceededAtUtc)
                    return true;

                var updated = current.Clone();
                updated.Status = FileProcessingStatus.DeleteSucceeded;
                updated.DeleteSucceededAt = deleteSucceededAtUtc;

                if (!cache.TryUpdate(fileKey, updated, current))
                    return false;

                IndexStatusCandidate(current, updated);

                lock (GetDatabaseLock(tenantId))
                {
                    var conn = GetDatabase(tenantId);
                    UpsertFile(conn, updated);
                }

                return true;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Removes a Processing file if it still matches the expected processing lease.
        /// Returns null when the record was removed or changed concurrently.
        /// </summary>
        public async Task<FileMetadata?> TryRemoveProcessingFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return null;

                if (current.Status != FileProcessingStatus.Processing
                    || !current.ProcessingStartTime.HasValue
                    || current.ProcessingStartTime.Value != expectedProcessingStartTimeUtc)
                {
                    return null;
                }

                var removed = ((ICollection<KeyValuePair<string, FileMetadata>>)cache)
                    .Remove(new KeyValuePair<string, FileMetadata>(fileKey, current));
                if (!removed)
                    return null;

                _fileKeyTenantIndex.TryRemove(fileKey, out _);
                RemovePhysicalPathCandidate(tenantId, fileKey, current.PhysicalPath);
                InvalidatePendingGeneration(tenantId, fileKey);
                EnqueuePersistence(new PersistenceOperation(tenantId, fileKey));
                _logger.LogDebug(
                    "Conditionally removed processing metadata for file: {FileKey}, Tenant: {TenantId}, LeaseStart: {LeaseStart}",
                    fileKey, tenantId, expectedProcessingStartTimeUtc);
                return current.Clone();
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Updates a Processing file if it still matches the expected processing lease.
        /// Returns null when the record was removed or changed concurrently.
        /// </summary>
        public async Task<FileMetadata?> TryUpdateProcessingFileAsync(
            string tenantId,
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            Func<FileMetadata, FileMetadata> updateFactory,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (updateFactory == null)
                throw new ArgumentNullException(nameof(updateFactory));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return null;

                if (current.Status != FileProcessingStatus.Processing
                    || !current.ProcessingStartTime.HasValue
                    || current.ProcessingStartTime.Value != expectedProcessingStartTimeUtc)
                {
                    return null;
                }

                var updated = updateFactory(current.Clone());
                if (!cache.TryUpdate(fileKey, updated, current))
                    return null;

                _fileKeyTenantIndex[fileKey] = tenantId;
                IndexPhysicalPathCandidate(tenantId, fileKey, current, updated);

                if (updated.Status == FileProcessingStatus.Pending)
                {
                    _pendingFileCounts.AddOrUpdate(tenantId, 1, (_, c) => c + 1);
                    EnqueuePendingCandidate(updated, refreshGeneration: false);
                }

                IndexStatusCandidate(current, updated);
                EnqueuePersistence(new PersistenceOperation(updated));
                _logger.LogDebug(
                    "Conditionally updated processing metadata for file: {FileKey}, Tenant: {TenantId}, LeaseStart: {LeaseStart}, NewStatus: {Status}",
                    fileKey, tenantId, expectedProcessingStartTimeUtc, updated.Status);
                return updated.Clone();
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Marks a Pending file as Processing if it still matches the expected pending state.
        /// Returns null when the record was removed or changed concurrently.
        /// </summary>
        public async Task<FileMetadata?> TryMarkPendingFileAsProcessingAsync(
            string tenantId,
            string fileKey,
            DateTime processingStartTimeUtc,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return null;

                if (current.Status != FileProcessingStatus.Pending)
                    return null;

                if (current.AvailableForProcessingAt.HasValue
                    && current.AvailableForProcessingAt.Value > processingStartTimeUtc)
                {
                    return null;
                }

                var updated = current.Clone();
                updated.Status = FileProcessingStatus.Processing;
                updated.ProcessingStartTime = processingStartTimeUtc;
                updated.AvailableForProcessingAt = null;

                if (!cache.TryUpdate(fileKey, updated, current))
                    return null;

                _fileKeyTenantIndex[fileKey] = tenantId;
                IndexPhysicalPathCandidate(tenantId, fileKey, current, updated);
                _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));
                InvalidatePendingGeneration(tenantId, fileKey);
                IndexStatusCandidate(current, updated);
                EnqueuePersistence(new PersistenceOperation(updated));
                _logger.LogDebug(
                    "Conditionally marked pending metadata as processing for file: {FileKey}, Tenant: {TenantId}, LeaseStart: {LeaseStart}",
                    fileKey, tenantId, processingStartTimeUtc);
                return updated.Clone();
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Resets a non-processing file back to Pending atomically under the tenant lock.
        /// Returns the current Processing snapshot unchanged when the file is still leased.
        /// Returns null when the record was removed concurrently.
        /// </summary>
        public async Task<FileMetadata?> TryResetFileToPendingAsync(
            string tenantId,
            string fileKey,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var cache = GetCache(tenantId);
                if (!cache.TryGetValue(fileKey, out var current))
                    return null;

                if (current.Status == FileProcessingStatus.Processing)
                    return current.Clone();

                var updated = current.Clone();
                updated.Status = FileProcessingStatus.Pending;
                updated.ProcessingStartTime = null;
                updated.AvailableForProcessingAt = null;
                updated.RetryCount = 0;
                updated.LastError = null;
                updated.LastFailedAt = null;

                if (!cache.TryUpdate(fileKey, updated, current))
                    return null;

                _fileKeyTenantIndex[fileKey] = tenantId;
                IndexPhysicalPathCandidate(tenantId, fileKey, current, updated);

                if (current.Status != FileProcessingStatus.Pending)
                    _pendingFileCounts.AddOrUpdate(tenantId, 1, (_, c) => c + 1);

                InvalidatePendingGeneration(tenantId, fileKey);
                EnqueuePendingCandidate(updated, refreshGeneration: false);
                IndexStatusCandidate(current, updated);
                EnqueuePersistence(new PersistenceOperation(updated));
                _logger.LogDebug(
                    "Conditionally reset file metadata to pending for file: {FileKey}, Tenant: {TenantId}, PreviousStatus: {PreviousStatus}",
                    fileKey, tenantId, current.Status);
                return updated.Clone();
            }
            finally
            {
                tenantLock.Release();
            }
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
        public Task<IEnumerable<FileMetadata>> GetByTenantAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var cache = GetCache(tenantId);
            var results = cache.Values.Select(m => m.Clone()).ToList();
            return Task.FromResult<IEnumerable<FileMetadata>>(results);
        }

        /// <summary>
        /// Returns a point-in-time reference snapshot of all metadata for a tenant without
        /// deep-cloning each entry.  Callers MUST NOT mutate the returned objects.
        /// This is O(N) reference copy vs the O(N �� clone_cost) of <see cref="GetByTenantAsync"/>
        /// and is intended for read-only, best-effort scans such as orphan cleanup.
        /// </summary>
        internal FileMetadata[] SnapshotTenantMetadataRaw(string tenantId)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // ToArray() materialises a point-in-time reference array so concurrent RemoveAsync
            // calls during iteration do not disturb the enumeration.
            return GetCache(tenantId).Values.ToArray();
        }

        /// <summary>
        /// Checks whether a metadata record exists for the specified physical path.
        /// Path comparison follows platform-specific case sensitivity.
        /// </summary>
        public Task<bool> ExistsByPhysicalPathAsync(string tenantId, string physicalPath, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(physicalPath))
                throw new ArgumentException("PhysicalPath cannot be empty", nameof(physicalPath));

            ct.ThrowIfCancellationRequested();
            var normalizedPath = NormalizePhysicalPath(physicalPath);
            if (normalizedPath == null)
                return Task.FromResult(false);

            return ExistsByNormalizedPhysicalPathAsync(tenantId, normalizedPath, ct);
        }

        internal Task<bool> ExistsByNormalizedPhysicalPathAsync(
            string tenantId,
            string normalizedPhysicalPath,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(normalizedPhysicalPath))
                throw new ArgumentException("Normalized physical path cannot be empty", nameof(normalizedPhysicalPath));

            ct.ThrowIfCancellationRequested();

            var cache = GetCache(tenantId);
            if (!_physicalPathIndex.TryGetValue(tenantId, out var tenantPathIndex))
                return Task.FromResult(false);

            if (!tenantPathIndex.TryGetValue(normalizedPhysicalPath, out var fileKey))
                return Task.FromResult(false);

            if (cache.TryGetValue(fileKey, out var metadata))
            {
                if (_pathComparer.Equals(metadata.PhysicalPath, normalizedPhysicalPath))
                    return Task.FromResult(true);

                var indexedMetadataPath = NormalizePhysicalPath(metadata.PhysicalPath);
                if (indexedMetadataPath != null && _pathComparer.Equals(indexedMetadataPath, normalizedPhysicalPath))
                    return Task.FromResult(true);
            }

            // Stale index entry (e.g., out-of-order concurrent update): self-heal on read.
            tenantPathIndex.TryRemove(normalizedPhysicalPath, out _);
            return Task.FromResult(false);
        }

        /// <summary>
        /// Gets a bounded batch of files in Processing status that timed out before the cutoff.
        /// </summary>
        public Task<IReadOnlyList<FileMetadata>> GetProcessingTimedOutAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (limit <= 0)
                return Task.FromResult<IReadOnlyList<FileMetadata>>(Array.Empty<FileMetadata>());

            ct.ThrowIfCancellationRequested();
            var results = GetStatusBatchFromIndex(
                tenantId,
                cutoffUtc,
                limit,
                FileProcessingStatus.Processing,
                _processingByStartTime,
                _processingByStartTimeLocks,
                metadata => metadata.ProcessingStartTime);

            return Task.FromResult(results);
        }

        /// <summary>
        /// Gets a bounded batch of files in PermanentlyFailed status whose last failure is older than the cutoff.
        /// </summary>
        public Task<IReadOnlyList<FileMetadata>> GetPermanentlyFailedOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            CancellationToken ct = default)
        {
            return GetPermanentlyFailedOlderThanAsync(tenantId, cutoffUtc, limit, excludedFileKeys: null, ct);
        }

        /// <summary>
        /// Gets a bounded batch of files in PermanentlyFailed status whose last failure is older than the cutoff.
        /// Excluded keys are deferred to later iterations so maintenance scans can progress past blocked records.
        /// </summary>
        public Task<IReadOnlyList<FileMetadata>> GetPermanentlyFailedOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (limit <= 0)
                return Task.FromResult<IReadOnlyList<FileMetadata>>(Array.Empty<FileMetadata>());

            ct.ThrowIfCancellationRequested();
            var results = GetStatusBatchFromIndex(
                tenantId,
                cutoffUtc,
                limit,
                FileProcessingStatus.PermanentlyFailed,
                _permanentlyFailedByLastFailedAt,
                _permanentlyFailedByLastFailedAtLocks,
                metadata => metadata.LastFailedAt,
                additionalPredicate: null,
                excludedFileKeys);

            return Task.FromResult(results);
        }

        /// <summary>
        /// Gets a bounded batch of files in Completed status whose completion time is older than the cutoff.
        /// Excluded keys are deferred to later iterations so maintenance scans can progress past blocked records.
        /// </summary>
        public Task<IReadOnlyList<FileMetadata>> GetCompletedOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (limit <= 0)
                return Task.FromResult<IReadOnlyList<FileMetadata>>(Array.Empty<FileMetadata>());

            ct.ThrowIfCancellationRequested();
            var results = GetStatusBatchFromIndex(
                tenantId,
                cutoffUtc,
                limit,
                FileProcessingStatus.Completed,
                _completedByCompletedAt,
                _completedByCompletedAtLocks,
                metadata => metadata.CompletedAt,
                metadata => !metadata.DeleteSucceededAt.HasValue,
                excludedFileKeys);

            return Task.FromResult(results);
        }

        /// <summary>
        /// Gets a bounded batch of files in DeleteRequested status whose completion time is older than the cutoff.
        /// Excluded keys are deferred to later iterations so maintenance scans can progress past blocked records.
        /// </summary>
        public Task<IReadOnlyList<FileMetadata>> GetDeleteRequestedOlderThanAsync(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            ISet<string>? excludedFileKeys = null,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (limit <= 0)
                return Task.FromResult<IReadOnlyList<FileMetadata>>(Array.Empty<FileMetadata>());

            ct.ThrowIfCancellationRequested();
            var results = GetStatusBatchFromIndex(
                tenantId,
                cutoffUtc,
                limit,
                FileProcessingStatus.DeleteRequested,
                _deleteRequestedByCompletedAt,
                _deleteRequestedByCompletedAtLocks,
                metadata => metadata.CompletedAt,
                metadata => !metadata.DeleteSucceededAt.HasValue,
                excludedFileKeys);

            return Task.FromResult(results);
        }

        /// <summary>
        /// Gets all file metadata with a specific status.
        /// </summary>
        public Task<IEnumerable<FileMetadata>> GetByStatusAsync(FileProcessingStatus status, CancellationToken ct = default)
        {
            var results = new List<FileMetadata>();

            foreach (var kvp in _activeFiles)
            {
                var tenantFiles = kvp.Value.Values.Where(m => m.Status == status).Select(m => m.Clone());
                results.AddRange(tenantFiles);
            }

            return Task.FromResult<IEnumerable<FileMetadata>>(results);
        }

        private void EnqueuePendingCandidate(FileMetadata metadata, bool refreshGeneration = true)
        {
            var generation = refreshGeneration
                ? IncrementPendingGeneration(metadata.TenantId, metadata.FileKey)
                : GetOrCreatePendingGeneration(metadata.TenantId, metadata.FileKey);
            var entry = new PendingQueueEntry(metadata.FileKey, generation);
            if (metadata.AvailableForProcessingAt.HasValue)
            {
                var availableAt = metadata.AvailableForProcessingAt.Value;
                if (availableAt > DateTime.UtcNow)
                {
                    EnqueueDelayed(metadata.TenantId, entry, availableAt);
                    return;
                }
            }

            var pendingQueue = _pendingKeys.GetOrAdd(metadata.TenantId, _ => new ConcurrentQueue<PendingQueueEntry>());
            pendingQueue.Enqueue(entry);
        }

        /// <inheritdoc/>
        public QueueProjectionWritePathStatistics GetWritePathStatisticsSnapshot()
        {
            return new QueueProjectionWritePathStatistics
            {
                ProjectedWriteCount = Interlocked.Read(ref _observedProjectionWriteCount),
                ValidationCount = Interlocked.Read(ref _observedProjectionValidationCount),
                ValidationTicks = Interlocked.Read(ref _observedProjectionValidationTicks),
                CacheAndIndexCount = Interlocked.Read(ref _observedProjectionCacheAndIndexCount),
                CacheAndIndexTicks = Interlocked.Read(ref _observedProjectionCacheAndIndexTicks),
                CacheMutationCount = Interlocked.Read(ref _observedProjectionCacheMutationCount),
                CacheMutationTicks = Interlocked.Read(ref _observedProjectionCacheMutationTicks),
                PhysicalPathIndexCount = Interlocked.Read(ref _observedProjectionPhysicalPathIndexCount),
                PhysicalPathIndexTicks = Interlocked.Read(ref _observedProjectionPhysicalPathIndexTicks),
                PendingQueueUpdateCount = Interlocked.Read(ref _observedProjectionPendingQueueUpdateCount),
                PendingQueueTicks = Interlocked.Read(ref _observedProjectionPendingQueueTicks),
                StatusIndexUpdateCount = Interlocked.Read(ref _observedProjectionStatusIndexUpdateCount),
                StatusIndexTicks = Interlocked.Read(ref _observedProjectionStatusIndexTicks),
                PersistenceEnqueueCount = Interlocked.Read(ref _observedProjectionPersistenceEnqueueCount),
                PersistenceEnqueueTicks = Interlocked.Read(ref _observedProjectionPersistenceEnqueueTicks)
            };
        }

        private static void RecordProjectionPhase(ref long count, ref long ticks, long elapsedTicks)
        {
            Interlocked.Increment(ref count);
            Interlocked.Add(ref ticks, elapsedTicks);
        }

        private long GetOrCreatePendingGeneration(string tenantId, string fileKey)
        {
            var generations = _pendingGenerations.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, long>(StringComparer.Ordinal));
            return generations.GetOrAdd(fileKey, 1);
        }

        private long IncrementPendingGeneration(string tenantId, string fileKey)
        {
            var generations = _pendingGenerations.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, long>(StringComparer.Ordinal));
            return generations.AddOrUpdate(fileKey, 1, (_, current) => current + 1);
        }

        private void InvalidatePendingGeneration(string tenantId, string fileKey)
        {
            if (_pendingGenerations.TryGetValue(tenantId, out var generations))
                generations.TryRemove(fileKey, out _);
        }

        private bool IsGenerationCurrent(string tenantId, PendingQueueEntry entry)
        {
            return _pendingGenerations.TryGetValue(tenantId, out var generations)
                && generations.TryGetValue(entry.FileKey, out var currentGeneration)
                && currentGeneration == entry.Generation;
        }

        private void RecordPendingDequeueStats(int attempts, int staleSkips)
        {
            if (attempts > 0)
                Interlocked.Add(ref _pendingDequeuedAttempts, attempts);
            if (staleSkips > 0)
                Interlocked.Add(ref _pendingDequeuedStaleSkips, staleSkips);
        }

        private void MaybeCompactPendingQueueForStaleRatio(
            string tenantId,
            ConcurrentDictionary<string, FileMetadata> cache,
            int staleSkips,
            int attempts)
        {
            if (attempts < StaleCompactionMinSamples || staleSkips <= 0)
                return;

            var staleRatio = staleSkips / (double)attempts;
            if (staleRatio < StaleCompactionRatioThreshold)
                return;

            CompactPendingQueueForTenant(tenantId, cache, force: true);

            if (Interlocked.Increment(ref _pendingStaleCompactionCounter) % 64 == 1)
            {
                _logger.LogInformation(
                    "Opportunistic pending queue compaction triggered for tenant {TenantId}: staleRatio={StaleRatio:P2}, staleSkips={StaleSkips}, attempts={Attempts}",
                    tenantId,
                    staleRatio,
                    staleSkips,
                    attempts);
            }
        }

        private void IndexPhysicalPathCandidate(
            string tenantId,
            string fileKey,
            FileMetadata? previous,
            FileMetadata current)
        {
            var normalizedCurrentPath = NormalizePhysicalPath(current.PhysicalPath);
            var normalizedPreviousPath = previous == null
                ? null
                : NormalizePhysicalPath(previous.PhysicalPath);

            var tenantPathIndex = _physicalPathIndex.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, string>(_pathComparer));

            if (normalizedPreviousPath != null
                && (normalizedCurrentPath == null || !_pathComparer.Equals(normalizedPreviousPath, normalizedCurrentPath))
                && tenantPathIndex.TryGetValue(normalizedPreviousPath, out var previousOwner)
                && string.Equals(previousOwner, fileKey, StringComparison.Ordinal))
            {
                tenantPathIndex.TryRemove(normalizedPreviousPath, out _);
            }

            if (normalizedCurrentPath != null)
            {
                tenantPathIndex[normalizedCurrentPath] = fileKey;
            }
        }

        private void RemovePhysicalPathCandidate(string tenantId, string fileKey, string? physicalPath)
        {
            if (physicalPath == null)
                return;

            var normalizedPath = NormalizePhysicalPath(physicalPath);
            if (normalizedPath == null)
                return;

            if (!_physicalPathIndex.TryGetValue(tenantId, out var tenantPathIndex))
                return;

            if (tenantPathIndex.TryGetValue(normalizedPath, out var existingFileKey)
                && string.Equals(existingFileKey, fileKey, StringComparison.Ordinal))
            {
                tenantPathIndex.TryRemove(normalizedPath, out _);
            }
        }

        private string? NormalizePhysicalPath(string? path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return null;

            try
            {
                return _fileSystem.Path.GetFullPath(path!);
            }
            catch
            {
                return path;
            }
        }

        private void IndexStatusCandidate(FileMetadata? previous, FileMetadata current)
        {
            var statusChanged = previous == null || previous.Status != current.Status;
            if (current.Status == FileProcessingStatus.Processing && current.ProcessingStartTime.HasValue)
            {
                if (statusChanged || previous?.ProcessingStartTime != current.ProcessingStartTime)
                {
                    EnqueueStatusIndex(
                        _processingByStartTime,
                        _processingByStartTimeLocks,
                        current.TenantId,
                        current.FileKey,
                        current.ProcessingStartTime.Value,
                        current.CreatedAt);
                }
            }

            if (current.Status == FileProcessingStatus.Completed && current.CompletedAt.HasValue)
            {
                if (statusChanged || previous?.CompletedAt != current.CompletedAt)
                {
                    EnqueueStatusIndex(
                        _completedByCompletedAt,
                        _completedByCompletedAtLocks,
                        current.TenantId,
                        current.FileKey,
                        current.CompletedAt.Value,
                        current.CreatedAt);
                }
            }

            if (current.Status == FileProcessingStatus.DeleteRequested && current.CompletedAt.HasValue)
            {
                if (statusChanged || previous?.CompletedAt != current.CompletedAt)
                {
                    EnqueueStatusIndex(
                        _deleteRequestedByCompletedAt,
                        _deleteRequestedByCompletedAtLocks,
                        current.TenantId,
                        current.FileKey,
                        current.CompletedAt.Value,
                        current.CreatedAt);
                }
            }

            if (current.Status == FileProcessingStatus.PermanentlyFailed && current.LastFailedAt.HasValue)
            {
                if (statusChanged || previous?.LastFailedAt != current.LastFailedAt)
                {
                    EnqueueStatusIndex(
                        _permanentlyFailedByLastFailedAt,
                        _permanentlyFailedByLastFailedAtLocks,
                        current.TenantId,
                        current.FileKey,
                        current.LastFailedAt.Value,
                        current.CreatedAt);
                }
            }
        }

        private void EnqueueStatusIndex(
            ConcurrentDictionary<string, StatusTimestampIndex> indexMap,
            ConcurrentDictionary<string, object> lockMap,
            string tenantId,
            string fileKey,
            DateTime timestampUtc,
            DateTime createdAtUtc)
        {
            var index = indexMap.GetOrAdd(tenantId, _ => new StatusTimestampIndex());
            var indexLock = lockMap.GetOrAdd(tenantId, _ => new object());
            var sequence = Interlocked.Increment(ref _statusIndexSequence);
            lock (indexLock)
            {
                index.Enqueue(fileKey, timestampUtc, createdAtUtc, sequence);
            }
        }

        // Maximum number of stale index entries to discard in a single GetStatusBatchFromIndex call.
        // Bounds the work done per cleanup call when the index contains many stale entries
        // (e.g. files that were deleted between index insertion and cleanup scan).
        // Stale entries that are not processed this cycle will be retried on the next cleanup run.
        private const int MaxStatusIndexStaleSkipsPerCall = 500;
        private const int MaxStatusIndexDeferredSkipsPerCall = 500;
        private const int MaxStatusIndexCandidatesPerCall = 5_000;

        private IReadOnlyList<FileMetadata> GetStatusBatchFromIndex(
            string tenantId,
            DateTime cutoffUtc,
            int limit,
            FileProcessingStatus expectedStatus,
            ConcurrentDictionary<string, StatusTimestampIndex> indexMap,
            ConcurrentDictionary<string, object> lockMap,
            Func<FileMetadata, DateTime?> timestampSelector,
            Func<FileMetadata, bool>? additionalPredicate = null,
            ISet<string>? excludedFileKeys = null)
        {
            if (!indexMap.TryGetValue(tenantId, out var index) || index.Count == 0)
                return Array.Empty<FileMetadata>();

            var cache = GetCache(tenantId);
            var indexLock = lockMap.GetOrAdd(tenantId, _ => new object());
            var results = new List<FileMetadata>(limit);
            var restoreEntries = new List<StatusTimestampEntry>(limit);
            var selectedKeys = new HashSet<string>(StringComparer.Ordinal);
            var staleSkips = 0;
            var deferredSkips = 0;
            var examined = 0;
            var maxCandidates = Math.Max(
                MaxStatusIndexCandidatesPerCall,
                limit + (excludedFileKeys?.Count ?? 0) + 64);

            // First lock section: dequeue candidates and validate them.
            // Valid entries (added to results) are collected in restoreEntries so they can be
            // re-enqueued for future cleanup cycles.  The re-enqueue is intentionally done in
            // a separate, shorter lock section below to reduce the time the lock is held.
            lock (indexLock)
            {
                while (results.Count < limit
                    && staleSkips < MaxStatusIndexStaleSkipsPerCall
                    && examined < maxCandidates
                    && index.TryPeek(out var peek))
                {
                    examined++;

                    if (peek.TimestampUtc >= cutoffUtc)
                        break;

                    if (!index.TryDequeue(out var candidate))
                        break;

                    if (!cache.TryGetValue(candidate.FileKey, out var metadata))
                    {
                        staleSkips++;
                        continue;
                    }

                    if (metadata.Status != expectedStatus)
                    {
                        staleSkips++;
                        continue;
                    }

                    var timestamp = timestampSelector(metadata);
                    if (!timestamp.HasValue)
                    {
                        staleSkips++;
                        continue;
                    }

                    if (timestamp.Value != candidate.TimestampUtc || timestamp.Value >= cutoffUtc)
                    {
                        staleSkips++;
                        continue;
                    }

                    if (!selectedKeys.Add(candidate.FileKey))
                    {
                        staleSkips++;
                        continue;
                    }

                    if (excludedFileKeys != null && excludedFileKeys.Contains(candidate.FileKey))
                    {
                        deferredSkips++;
                        restoreEntries.Add(candidate);

                        if (deferredSkips >= MaxStatusIndexDeferredSkipsPerCall
                            && results.Count > 0)
                        {
                            break;
                        }

                        continue;
                    }

                    if (additionalPredicate != null && !additionalPredicate(metadata))
                    {
                        staleSkips++;
                        continue;
                    }

                    results.Add(metadata);
                    restoreEntries.Add(candidate);
                }
            }

            // Second lock section: re-enqueue selected entries so future cleanup cycles can
            // find them again.  Splitting into a separate lock acquisition lets other threads
            // (e.g. pending-file allocators) progress while we are not touching the heap.
            if (restoreEntries.Count > 0)
            {
                lock (indexLock)
                {
                    foreach (var entry in restoreEntries)
                    {
                        index.Enqueue(
                            entry.FileKey,
                            entry.TimestampUtc,
                            entry.CreatedAtUtc,
                            Interlocked.Increment(ref _statusIndexSequence));
                    }
                }
            }

            if (staleSkips > 0)
                Interlocked.Add(ref _statusIndexStaleSkips, staleSkips);

            return results;
        }

        // Maximum number of delayed items promoted to ready per allocation call.
        // Caps lock hold time when many delayed entries are due simultaneously.
        private const int MaxDelayedPromotions = 64;
        private const int MaxDelayedPromotionsBatch = 256;
        private const int MaxSinglePendingDequeues = 512;
        private const int StaleCompactionMinSamples = 32;
        private const double StaleCompactionRatioThreshold = 0.5;

        private void EnqueueDelayed(string tenantId, PendingQueueEntry entry, DateTime availableAtUtc)
        {
            var delayedQueue = _delayedQueues.GetOrAdd(tenantId, _ => new DelayedQueue());
            var delayedLock = _delayedQueueLocks.GetOrAdd(tenantId, _ => new object());
            var sequence = Interlocked.Increment(ref _delayedSequence);

            lock (delayedLock)
            {
                delayedQueue.Enqueue(entry.FileKey, entry.Generation, availableAtUtc, sequence);
            }
        }

        private void PromoteDelayedReady(
            string tenantId,
            DateTime nowUtc,
            ConcurrentQueue<PendingQueueEntry> readyQueue,
            int maxPromotions)
        {
            if (!_delayedQueues.TryGetValue(tenantId, out var delayedQueue))
                return;

            var delayedLock = _delayedQueueLocks.GetOrAdd(tenantId, _ => new object());
            var promoted = 0;

            lock (delayedLock)
            {
                while (promoted < maxPromotions && delayedQueue.TryDequeueReady(nowUtc, out var entry))
                {
                    readyQueue.Enqueue(entry);
                    promoted++;
                }
            }
        }

        /// <summary>
        /// Gets the next pending file for processing (atomic, O(1) amortized).
        /// Returns null if no files are currently available.
        /// Thread-safe: Uses per-tenant lock for atomic dequeue-and-mark.
        ///
        /// Uses a FIFO ConcurrentQueue. Stale entries (files no longer Pending in cache)
        /// are validated and discarded on dequeue. Delayed-retry files are held in a separate
        /// delayed queue and promoted to ready when they become due.
        /// </summary>
        public async Task<FileMetadata?> GetNextPendingFileAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (TryTakePrefetchedPending(tenantId, out var prefetched))
                return prefetched;

            // Fast-path: if no pending files, skip taking the tenant lock.
            if (_pendingFileCounts.TryGetValue(tenantId, out var pendingCount) && pendingCount <= 0)
            {
                var gate = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
                if (gate.Wait(0))
                {
                    gate.Release();
                    // Re-check prefetch buffer: a concurrent writer may have enqueued an entry
                    // between the pendingCount check above and the gate acquisition.
                    if (TryTakePrefetchedPending(tenantId, out prefetched))
                        return prefetched;
                    return null;
                }

                await gate.WaitAsync(ct);
                gate.Release();

                if (TryTakePrefetchedPending(tenantId, out prefetched))
                    return prefetched;

                if (_pendingFileCounts.TryGetValue(tenantId, out pendingCount) && pendingCount <= 0)
                    return null;
            }

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            ConcurrentDictionary<string, FileMetadata>? cacheRef = null;
            var lockContended = !tenantLock.Wait(0);
            if (lockContended)
                await tenantLock.WaitAsync(ct);

            var maxAllocations = lockContended ? PendingSingleAllocatorPrefetchCount : 1;
            var transitions = new List<(FileMetadata Previous, FileMetadata Updated)>(maxAllocations);
            var dequeuedAttempts = 0;
            var staleSkips = 0;
            FileMetadata? prefetchedInsideLock = null;
            try
            {
                if (TryTakePrefetchedPending(tenantId, out var prefetchedUnderLock))
                {
                    prefetchedInsideLock = prefetchedUnderLock;
                }

                var cache = GetCache(tenantId);
                cacheRef = cache;
                var pendingQueue = _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<PendingQueueEntry>());
                var now = DateTime.UtcNow;
                PromoteDelayedReady(tenantId, now, pendingQueue, MaxDelayedPromotions);
                pendingCount = _pendingFileCounts.TryGetValue(tenantId, out var currentPending) ? currentPending : 0;
                var maxDequeues = Math.Min(Math.Max(pendingCount + 64, 64), MaxSinglePendingDequeues);
                var targetAllocations = Math.Min(Math.Max(1, pendingCount), maxAllocations);

                while (prefetchedInsideLock == null
                       && dequeuedAttempts++ < maxDequeues
                       && transitions.Count < targetAllocations
                       && pendingQueue.TryDequeue(out var entry))
                {
                    if (!IsGenerationCurrent(tenantId, entry))
                    {
                        staleSkips++;
                        continue;
                    }

                    if (!cache.TryGetValue(entry.FileKey, out var candidate))
                    {
                        staleSkips++;
                        continue;
                    }

                    if (candidate.Status != FileProcessingStatus.Pending)
                    {
                        staleSkips++;
                        continue;
                    }

                    if (candidate.AvailableForProcessingAt.HasValue
                        && candidate.AvailableForProcessingAt.Value > now)
                    {
                        // Not ready yet - move back to delayed queue.
                        EnqueueDelayed(tenantId, entry, candidate.AvailableForProcessingAt.Value);
                        continue;
                    }

                    // Clone before mutating so concurrent lock-free readers always observe
                    // a fully-consistent object (Pending snapshot or Processing snapshot).
                    var updated = candidate.Clone();
                    updated.Status = FileProcessingStatus.Processing;
                    updated.ProcessingStartTime = now;

                    cache[updated.FileKey] = updated;
                    _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));
                    InvalidatePendingGeneration(tenantId, updated.FileKey);
                    transitions.Add((candidate, updated));
                }

                for (var i = 1; i < transitions.Count; i++)
                    EnqueuePrefetchedPending(tenantId, transitions[i].Updated);
            }
            finally
            {
                tenantLock.Release();
            }

            // Non-critical follow-up work is intentionally done outside the tenant lock
            // to reduce contention among concurrent allocators for the same tenant.
            RecordPendingDequeueStats(dequeuedAttempts, staleSkips);
            if (cacheRef != null)
                MaybeCompactPendingQueueForStaleRatio(tenantId, cacheRef, staleSkips, dequeuedAttempts);

            if (prefetchedInsideLock != null)
                return prefetchedInsideLock;

            if (transitions.Count == 0)
                return null;

            for (var i = 0; i < transitions.Count; i++)
            {
                var transition = transitions[i];
                IndexStatusCandidate(transition.Previous, transition.Updated);
                EnqueuePersistence(new PersistenceOperation(transition.Updated));
            }

            var selected = transitions[0].Updated;
            _logger.LogDebug("Allocated file for processing: {FileKey}, Tenant: {TenantId}", selected.FileKey, tenantId);
            return selected;
        }
        /// <summary>
        /// Gets a batch of pending files for processing (atomic operation).
        /// Thread-safe: Uses the same per-tenant SemaphoreSlim as GetNextPendingFileAsync
        /// to ensure concurrent callers cannot receive the same files.
        /// </summary>
        public async Task<IEnumerable<FileMetadata>> GetNextPendingBatchAsync(
            string tenantId,
            int batchSize,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (batchSize <= 0)
                throw new ArgumentException("Batch size must be greater than zero", nameof(batchSize));

            FileMetadata prefetched;
            var results = new List<FileMetadata>(batchSize);
            while (results.Count < batchSize && TryTakePrefetchedPending(tenantId, out prefetched))
                results.Add(prefetched);

            if (results.Count >= batchSize)
                return results;

            // Fast-path: if no pending files, skip taking the tenant lock.
            if (_pendingFileCounts.TryGetValue(tenantId, out var pendingCount) && pendingCount <= 0)
            {
                var gate = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
                if (!gate.Wait(0))
                {
                    await gate.WaitAsync(ct);
                    gate.Release();

                    while (results.Count < batchSize && TryTakePrefetchedPending(tenantId, out prefetched))
                        results.Add(prefetched);

                    if (results.Count >= batchSize)
                        return results;
                }
                else
                {
                    gate.Release();
                }

                if (_pendingFileCounts.TryGetValue(tenantId, out pendingCount) && pendingCount <= 0)
                    return results;
            }

            // Get or create tenant-specific lock (shared with GetNextPendingFileAsync)
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));

            ConcurrentDictionary<string, FileMetadata>? cacheRef = null;
            var transitions = new List<(FileMetadata Previous, FileMetadata Updated)>(batchSize);
            var dequeued = 0;
            var staleSkips = 0;

            // Acquire lock to ensure atomic dequeue-and-mark across concurrent callers
            await tenantLock.WaitAsync(ct);
            try
            {
                while (results.Count < batchSize && TryTakePrefetchedPending(tenantId, out prefetched))
                    results.Add(prefetched);

                if (results.Count >= batchSize)
                    return results;

                var cache = GetCache(tenantId);
                cacheRef = cache;
                var pendingQueue = _pendingKeys.GetOrAdd(tenantId, _ => new ConcurrentQueue<PendingQueueEntry>());
                var now = DateTime.UtcNow;
                PromoteDelayedReady(tenantId, now, pendingQueue, MaxDelayedPromotionsBatch);

                // Cap total dequeues to avoid an unbounded loop when many stale/delayed entries
                // are present. Tie the cap to pendingCount and batchSize to bound lock hold time.
                pendingCount = _pendingFileCounts.TryGetValue(tenantId, out var currentPending) ? currentPending : 0;
                var targetBatchSize = batchSize - results.Count;
                var maxDequeues = Math.Min(pendingCount + 64, targetBatchSize * 2 + 64);

                while (transitions.Count < targetBatchSize
                       && dequeued++ < maxDequeues
                       && pendingQueue.TryDequeue(out var entry))
                {
                    if (!IsGenerationCurrent(tenantId, entry))
                    {
                        staleSkips++;
                        continue;
                    }

                    if (!cache.TryGetValue(entry.FileKey, out var candidate))
                    {
                        staleSkips++;
                        continue;
                    }

                    if (candidate.Status != FileProcessingStatus.Pending)
                    {
                        staleSkips++;
                        continue;
                    }

                    if (candidate.AvailableForProcessingAt.HasValue
                        && candidate.AvailableForProcessingAt.Value > now)
                    {
                        // Not ready yet - move back to delayed queue.
                        EnqueueDelayed(tenantId, entry, candidate.AvailableForProcessingAt.Value);
                        continue;
                    }

                    // Clone before mutating - same rationale as GetNextPendingFileAsync.
                    var updated = candidate.Clone();
                    updated.Status = FileProcessingStatus.Processing;
                    updated.ProcessingStartTime = now;

                    cache[updated.FileKey] = updated;
                    _pendingFileCounts.AddOrUpdate(tenantId, 0, (_, c) => Math.Max(0, c - 1));
                    InvalidatePendingGeneration(tenantId, updated.FileKey);
                    transitions.Add((candidate, updated));
                }
            }
            finally
            {
                tenantLock.Release();
            }

            // Non-critical follow-up work is intentionally done outside the tenant lock
            // to reduce contention among concurrent allocators for the same tenant.
            for (var i = 0; i < transitions.Count; i++)
            {
                var transition = transitions[i];
                IndexStatusCandidate(transition.Previous, transition.Updated);
                EnqueuePersistence(new PersistenceOperation(transition.Updated));
                results.Add(transition.Updated);
            }

            RecordPendingDequeueStats(dequeued, staleSkips);
            if (cacheRef != null)
                MaybeCompactPendingQueueForStaleRatio(tenantId, cacheRef, staleSkips, dequeued);

            _logger.LogDebug("Allocated {Count} files for processing, Tenant: {TenantId}", results.Count, tenantId);
            return results;
        }

        private bool TryTakePrefetchedPending(string tenantId, out FileMetadata metadata)
        {
            metadata = null!;
            if (!_prefetchedPendingByTenant.TryGetValue(tenantId, out var queue))
                return false;

            if (queue.IsEmpty)
                return false;

            var cache = GetCache(tenantId);
            while (queue.TryDequeue(out var item))
            {
                if (cache.TryGetValue(item.FileKey, out var current)
                    && current.Status == FileProcessingStatus.Processing
                    && current.ProcessingStartTime.HasValue
                    && item.ProcessingStartTime.HasValue
                    && current.ProcessingStartTime.Value == item.ProcessingStartTime.Value)
                {
                    metadata = current;
                    return true;
                }
            }

            return false;
        }

        private void EnqueuePrefetchedPending(string tenantId, FileMetadata metadata)
        {
            var queue = _prefetchedPendingByTenant.GetOrAdd(
                tenantId,
                _ => new ConcurrentQueue<FileMetadata>());
            queue.Enqueue(metadata);
        }
        /// <summary>
        /// Resets files that have been in Processing status for longer than the timeout.
        /// Uses Write-Behind (same as AddOrUpdateAsync) for consistency with the rest of the
        /// repository: memory is updated first, SQLite persistence is async via the background loop.
        /// </summary>
        /// <remarks>
        /// This method performs a bulk scan across all tenants. Production code should prefer
        /// <see cref="TryResetTimedOutFileAsync"/> which operates per-file and is called by
        /// <see cref="StorageCleanupService"/> after its own timed-out-file scan.
        /// </remarks>
        [Obsolete("Use TryResetTimedOutFileAsync for per-file timeout resets. This bulk method is not called by production code.")]
        public Task<int> ResetTimedOutFilesAsync(TimeSpan timeout, CancellationToken ct = default)
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

                    // Clone before mutating - keeps the cache entry consistent for lock-free readers.
                    var updated = file.Clone();
                    updated.Status = FileProcessingStatus.Pending;
                    updated.ProcessingStartTime = null;
                    updated.AvailableForProcessingAt = null;

                    // Use TryUpdate (CAS) so that if a concurrent GetNextPendingFileAsync or
                    // MarkAsFailedAsync already changed this entry since we read it above, we
                    // skip rather than overwriting with stale data. The `file` reference acts
                    // as the expected-current-value sentinel.
                    if (!cache.TryUpdate(updated.FileKey, updated, file))
                        continue;

                    _pendingFileCounts.AddOrUpdate(tenantId, 1, (_, c) => c + 1);
                    IndexStatusCandidate(file, updated);
                    EnqueuePersistence(new PersistenceOperation(updated));

                    // Re-enqueue in the pending queue so the allocator can find it immediately.
                    EnqueuePendingCandidate(updated, refreshGeneration: false);

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
        public Task<FileMetadata?> GetByFileKeyAsync(string fileKey, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (TryResolveTenantIdByFileKey(fileKey, out var tenantId)
                && _activeFiles.TryGetValue(tenantId, out var indexedTenantFiles)
                && indexedTenantFiles.TryGetValue(fileKey, out var indexedMetadata))
            {
                return Task.FromResult<FileMetadata?>(indexedMetadata.Clone());
            }

            return Task.FromResult<FileMetadata?>(null);
        }

        internal bool TryResolveTenantIdByFileKey(string fileKey, out string tenantId)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            if (_fileKeyTenantIndex.TryGetValue(fileKey, out tenantId)
                && _activeFiles.TryGetValue(tenantId, out var indexedTenantFiles)
                && indexedTenantFiles.ContainsKey(fileKey))
            {
                return true;
            }

            // Fallback: recover from stale/missing index entries (rare, e.g. after crash recovery).
            foreach (var tenantEntry in _activeFiles)
            {
                if (!tenantEntry.Value.ContainsKey(fileKey))
                    continue;

                tenantId = tenantEntry.Key;
                _fileKeyTenantIndex[fileKey] = tenantId;
                return true;
            }

            _fileKeyTenantIndex.TryRemove(fileKey, out _);
            tenantId = null!;
            return false;
        }

        /// <summary>
        /// Gets all file metadata (for maintenance operations).
        /// </summary>
        public Task<IEnumerable<FileMetadata>> GetAllAsync(CancellationToken ct = default)
        {
            var results = new List<FileMetadata>();

            foreach (var kvp in _activeFiles)
            {
                results.AddRange(kvp.Value.Values.Select(m => m.Clone()));
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
        public Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();
            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, tenantId, "metadata.db");

            long sizeBefore = 0;
            long sizeAfter = 0;
            try
            {
                if (!_fileSystem.File.Exists(dbPath))
                    return Task.FromResult((0L, 0L));

                sizeBefore = _fileSystem.FileInfo.New(dbPath).Length;

                // VACUUM requires exclusive write access to the database file.
                // Repository connections use Pooling=False, so disposing the cached tenant
                // connection releases the underlying file handle without disturbing other tenants.
                lock (GetDatabaseLock(tenantId))
                {
                    // Step 1: evict the cached connection so GetDatabase rebuilds it afterwards.
                    _databases.TryRemove(tenantId, out var lazyConn);
                    if (lazyConn != null && lazyConn.IsValueCreated)
                    {
                        try { lazyConn.Value?.Dispose(); }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error closing metadata connection before VACUUM for tenant {TenantId}", tenantId);
                        }
                    }

                    // Step 2: run VACUUM on a fresh dedicated connection.
                    var vacuumConnStr = _sqliteOptions.BuildConnectionString(dbPath);
                    using (var vacuumConn = new SqliteConnection(vacuumConnStr))
                    {
                        vacuumConn.Open();
                        using (var cmd = vacuumConn.CreateCommand())
                        {
                            cmd.CommandText = "VACUUM;";
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Step 3: the dedicated connection is disposed here; GetDatabase will open
                    // a fresh tenant connection lazily on the next access.
                }

                if (_fileSystem.File.Exists(dbPath))
                    sizeAfter = _fileSystem.FileInfo.New(dbPath).Length;

                _logger.LogDebug("SQLite VACUUM completed for tenant {TenantId}: {Before} -> {After} bytes", tenantId, sizeBefore, sizeAfter);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to optimize SQLite database for tenant {TenantId}", tenantId);
            }

            return Task.FromResult((sizeBefore, sizeAfter));
        }

        /// <summary>
        /// Gets all tenant IDs that have metadata databases.
        /// </summary>
        public async Task<IEnumerable<string>> GetAllTenantIdsAsync(CancellationToken ct = default)
        {
            // Include in-memory tenants first so cleanup can process recent writes that have not
            // been flushed to disk yet by the write-behind persistence loop.
            var tenantIds = new HashSet<string>(_activeFiles.Keys, StringComparer.Ordinal);

            foreach (var tenantId in GetTenantDirectoryIdsSnapshot())
            {
                if (!string.IsNullOrWhiteSpace(tenantId))
                    tenantIds.Add(tenantId);
            }

            return tenantIds;
        }

        private string[] GetTenantDirectoryIdsSnapshot()
        {
            if (!_fileSystem.Directory.Exists(_metadataDirectory))
                return Array.Empty<string>();

            var now = Stopwatch.GetTimestamp();
            var lastRefresh = Interlocked.Read(ref _cachedTenantDirectoryIdsTicks);
            if (lastRefresh != 0 && (now - lastRefresh) < TenantDirectoryCacheTicks)
                return _cachedTenantDirectoryIds;

            var tenantDirs = _fileSystem.Directory.GetDirectories(_metadataDirectory);
            var tenantIds = tenantDirs
                .Select(dir => _fileSystem.Path.GetFileName(dir))
                .Where(tid => !string.IsNullOrWhiteSpace(tid))
                .ToArray()!;

            _cachedTenantDirectoryIds = tenantIds;
            Interlocked.Exchange(ref _cachedTenantDirectoryIdsTicks, now);
            return tenantIds;
        }

        private void RemoveTenantFromGlobalIndex(ConcurrentDictionary<string, FileMetadata> tenantCache)
        {
            foreach (var fileKey in tenantCache.Keys)
                _fileKeyTenantIndex.TryRemove(fileKey, out _);
        }

        /// <summary>
        /// Deletes SQLite WAL and SHM sidecar files for a given database path.
        /// Called after deleting a corrupted database so the new database starts clean.
        /// </summary>
        private void DeleteSidecarFiles(string dbPath)
        {
            foreach (var suffix in new[] { "-wal", "-shm" })
            {
                var sidecarPath = dbPath + suffix;
                if (_fileSystem.File.Exists(sidecarPath))
                {
                    try
                    {
                        _fileSystem.File.Delete(sidecarPath);
                        _logger.LogInformation("Deleted SQLite sidecar file: {Path}", sidecarPath);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete SQLite sidecar file: {Path}", sidecarPath);
                    }
                }
            }
        }

        /// <summary>
        /// Checks whether an exception indicates a recoverable database corruption scenario.
        /// Consistent with DirectoryQuotaRepository.IsRecoverableDatabaseException.
        /// </summary>
        private static bool IsRecoverableDatabaseException(Exception ex)
        {
            if (ex is IOException)
                return true;

            if (ex is SqliteException sqliteEx)
            {
                // SQLITE_CORRUPT (11), SQLITE_NOTADB (26), SQLITE_IOERR (10)
                return sqliteEx.SqliteErrorCode == 11
                    || sqliteEx.SqliteErrorCode == 26
                    || sqliteEx.SqliteErrorCode == 10;
            }

            return false;
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
            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, tenantId, "metadata.db");

            if (!_fileSystem.File.Exists(dbPath))
            {
                _logger.LogInformation("No database file to rebuild for tenant {TenantId}", tenantId);
                return null;
            }

            // IMPORTANT: Do NOT acquire GetDatabaseLock here.
            // This method is only called from within the Lazy<SqliteConnection> factory in GetDatabase,
            // which holds the Lazy.ExecutionAndPublication lock for this tenant.
            // If we try to acquire GetDatabaseLock we risk a deadlock:
            //   Thread A: inside Lazy factory (holds Lazy lock) �� wants GetDatabaseLock
            //   Thread B: inside AddOrUpdateDirectAsync (holds GetDatabaseLock) �� waits on Lazy.Value
            // The Lazy lock already serializes all concurrent callers for this tenantId, so all
            // ConcurrentDictionary updates and file operations below are safe without an extra lock.
            {
                // Remove from cache FIRST so no concurrent thread can acquire the disposed instance.
                _databases.TryRemove(tenantId, out var existingLazy);

                // Dispose the removed connection (safe: it's no longer reachable via cache).
                if (existingLazy != null && existingLazy.IsValueCreated)
                {
                    try { existingLazy.Value?.Dispose(); }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error disposing SQLite connection during lock-free rebuild for tenant {TenantId}", tenantId);
                    }
                }

                if (_activeFiles.TryRemove(tenantId, out var staleTenantCache))
                    RemoveTenantFromGlobalIndex(staleTenantCache);
                _physicalPathIndex.TryRemove(tenantId, out _);
                _pendingKeys.TryRemove(tenantId, out _);
                _pendingGenerations.TryRemove(tenantId, out _);
                _prefetchedPendingByTenant.TryRemove(tenantId, out _);
                _pendingFileCounts.TryRemove(tenantId, out _);
                _delayedQueues.TryRemove(tenantId, out _);
                _delayedQueueLocks.TryRemove(tenantId, out _);
                _processingByStartTime.TryRemove(tenantId, out _);
                _processingByStartTimeLocks.TryRemove(tenantId, out _);
            _completedByCompletedAt.TryRemove(tenantId, out _);
            _completedByCompletedAtLocks.TryRemove(tenantId, out _);
            _deleteRequestedByCompletedAt.TryRemove(tenantId, out _);
            _deleteRequestedByCompletedAtLocks.TryRemove(tenantId, out _);
            _permanentlyFailedByLastFailedAt.TryRemove(tenantId, out _);
            _permanentlyFailedByLastFailedAtLocks.TryRemove(tenantId, out _);

                var backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
                _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
                _logger.LogInformation("Backed up corrupted database: {BackupPath}", backupPath);

                _fileSystem.File.Delete(dbPath);
                _logger.LogInformation("Deleted corrupted database: {DatabasePath}", dbPath);

                // Also delete SQLite WAL and SHM sidecar files so the new database starts clean.
                DeleteSidecarFiles(dbPath);

                return backupPath;
            }
        }

        /// <summary>
        /// A scoped handle returned by <see cref="BeginDatabaseRebuildAsync"/> that holds
        /// the exclusive tenant lock for the duration of a database rebuild operation.
        /// Disposing this handle releases the lock and unblocks all pending operations
        /// for the tenant. Use with a <c>using</c> declaration to guarantee release.
        /// </summary>
        public sealed class DatabaseRebuildLockHandle : IDisposable
        {
            private readonly MetadataRepository _owner;
            private readonly string _tenantId;
            private int _disposed;

            /// <summary>Path to the backup of the corrupted database, or null if no database existed.</summary>
            public string? BackupPath { get; }

            internal DatabaseRebuildLockHandle(MetadataRepository owner, string tenantId, string? backupPath)
            {
                _owner = owner;
                _tenantId = tenantId;
                BackupPath = backupPath;
            }

            /// <summary>
            /// Releases the exclusive tenant lock, unblocking all pending operations.
            /// Safe to call multiple times (idempotent).
            /// </summary>
            public void Dispose()
            {
                if (System.Threading.Interlocked.Exchange(ref _disposed, 1) == 0)
                    _owner.FinishDatabaseRebuild(_tenantId);
            }
        }

        /// <summary>
        /// Prepares for database rebuild by safely disposing connections and backing up the database.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations.
        /// Returns a <see cref="DatabaseRebuildLockHandle"/> that MUST be disposed (via a
        /// <c>using</c> declaration) to release the lock and unblock operations for the tenant.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>
        /// A handle whose <see cref="DatabaseRebuildLockHandle.BackupPath"/> is the path to the
        /// corrupted database backup, or <c>null</c> if no database file existed.
        /// </returns>
        public async Task<DatabaseRebuildLockHandle> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var dbPath = _fileSystem.Path.Combine(_metadataDirectory, tenantId, "metadata.db");

            // Get or create tenant lock (same lock used for all operations)
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));

            // Acquire exclusive lock - this will block all operations for this tenant.
            // The lock is released when the caller disposes the returned handle.
            await tenantLock.WaitAsync(ct);

            try
            {
                // If database doesn't exist, nothing to backup. Return a handle with null BackupPath;
                // the lock is still held so the caller can safely create a fresh database.
                if (!_fileSystem.File.Exists(dbPath))
                {
                    _logger.LogInformation("No database file to rebuild for tenant {TenantId}", tenantId);
                    return new DatabaseRebuildLockHandle(this, tenantId, backupPath: null);
                }

                _logger.LogWarning("Beginning database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);

                string backupPath;
                lock (GetDatabaseLock(tenantId))
                {
                    // Step 1: Remove from cache FIRST so no other thread can acquire the disposed instance.
                    _databases.TryRemove(tenantId, out var lazyConn);

                    // Step 2: Dispose the removed connection (safe: it's no longer reachable via cache).
                    if (lazyConn != null && lazyConn.IsValueCreated)
                    {
                        try
                        {
                            lazyConn.Value?.Dispose();
                            _logger.LogDebug("Disposed SQLite connection for rebuild: {TenantId}", tenantId);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error disposing SQLite connection for tenant {TenantId}", tenantId);
                        }
                    }

                    // Step 3: Clear in-memory cache and pending index
                    if (_activeFiles.TryRemove(tenantId, out var staleTenantCache))
                        RemoveTenantFromGlobalIndex(staleTenantCache);
                    _physicalPathIndex.TryRemove(tenantId, out _);
                    _pendingKeys.TryRemove(tenantId, out _);
                    _pendingGenerations.TryRemove(tenantId, out _);
                    _prefetchedPendingByTenant.TryRemove(tenantId, out _);
                    _pendingFileCounts.TryRemove(tenantId, out _);
                    _delayedQueues.TryRemove(tenantId, out _);
                    _delayedQueueLocks.TryRemove(tenantId, out _);
                    _processingByStartTime.TryRemove(tenantId, out _);
                    _processingByStartTimeLocks.TryRemove(tenantId, out _);
            _completedByCompletedAt.TryRemove(tenantId, out _);
            _completedByCompletedAtLocks.TryRemove(tenantId, out _);
            _deleteRequestedByCompletedAt.TryRemove(tenantId, out _);
            _deleteRequestedByCompletedAtLocks.TryRemove(tenantId, out _);
            _permanentlyFailedByLastFailedAt.TryRemove(tenantId, out _);
            _permanentlyFailedByLastFailedAtLocks.TryRemove(tenantId, out _);

                    // Step 4: Backup corrupted database
                    backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
                    _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
                    _logger.LogInformation("Backed up corrupted database: {BackupPath}", backupPath);

                    // Step 5: Delete corrupted database
                    _fileSystem.File.Delete(dbPath);
                    _logger.LogInformation("Deleted corrupted database: {DatabasePath}", dbPath);

                    // Also delete SQLite WAL and SHM sidecar files so the new database starts clean.
                    DeleteSidecarFiles(dbPath);
                }

                return new DatabaseRebuildLockHandle(this, tenantId, backupPath);
            }
            catch
            {
                // If anything fails, release the lock immediately before propagating the exception.
                tenantLock.Release();
                throw;
            }
        }

        /// <summary>
        /// Releases the exclusive tenant lock acquired by <see cref="BeginDatabaseRebuildAsync"/>.
        /// Called automatically by <see cref="DatabaseRebuildLockHandle.Dispose"/>;
        /// callers should not invoke this directly.
        /// </summary>
        private void FinishDatabaseRebuild(string tenantId)
        {
            if (_tenantLocks.TryGetValue(tenantId, out var tenantLock))
            {
                tenantLock.Release();
                _logger.LogInformation("Database rebuild completed for tenant {TenantId}. Operations unblocked.", tenantId);
            }
        }

        // Maximum number of operations allowed in the persistence queue.
        // If SQLite is unavailable for an extended period, the queue may fill up. When near or
        // at capacity, operations are coalesced by file key so only the latest state is retained.
        private const int DefaultMaxPersistenceQueueSize = 100_000;
        private const int DefaultDrainBatchSize = 2_000;
        private const int DefaultPersistenceQueueSoftMergeThresholdPercent = 90;
        private const int DefaultStartupLoadBatchSize = 2_000;
        private const int DefaultShutdownDrainTimeoutSeconds = 30;
        private const int DefaultPersistenceIntervalSeconds = 2;
        private const int PendingSingleAllocatorPrefetchCount = 4;

        // Compact pending queues every N drain cycles to reclaim memory from stale entries.
        // At ~one drain-cycle per 5 s, 20 cycles ~= every ~100 seconds.
        // A shorter interval limits queue bloat when files are retried frequently,
        // reducing the window in which a retried file is delayed from being re-allocated.
        private const int CompactionIntervalCycles = 20;

        // Enqueues an operation for background persistence.
        // If the queue is full (SQLite has been unavailable for a long time), the operation
        // is dropped with a warning - memory state is already correct, so data is not lost
        // within this process lifetime. On restart, the cleanup service reconciles disk vs metadata.
        private void EnqueuePersistence(PersistenceOperation op)
        {
            // Used by unit tests to avoid orphaned background workers.
            // When disabled, persist inline and skip queueing/task signaling.
            if (!_enableBackgroundPersistence)
            {
                _ = ExecuteBatch(op.TenantId, new List<PersistenceOperation> { op });
                return;
            }

            if (TryWritePersistenceOperation(op))
                return;

            var queueDepth = Volatile.Read(ref _persistenceChannelDepth);
            if (queueDepth >= _persistenceQueueSoftMergeThreshold)
            {
                CoalescePersistenceOperation(op, queueDepth, "near_capacity");
                return;
            }

            CoalescePersistenceOperation(op, queueDepth, "queue_full");
        }

        private bool TryWritePersistenceOperation(PersistenceOperation op)
        {
            Interlocked.Increment(ref _persistenceChannelDepth);
            if (!_persistenceWriter.TryWrite(op))
            {
                Interlocked.Decrement(ref _persistenceChannelDepth);
                return false;
            }

            UpdatePeakPersistenceDepth(Volatile.Read(ref _persistenceChannelDepth));
            return true;
        }

        private void UpdatePeakPersistenceDepth(int currentDepth)
        {
            while (true)
            {
                var observedPeak = Volatile.Read(ref _persistenceChannelPeakDepth);
                if (observedPeak >= currentDepth)
                    return;

                if (Interlocked.CompareExchange(ref _persistenceChannelPeakDepth, currentDepth, observedPeak) == observedPeak)
                    return;
            }
        }

        private void CoalescePersistenceOperation(PersistenceOperation op, int queueDepth, string reason)
        {
            var compositeKey = $"{op.TenantId}\u001F{op.FileKey}";
            var added = _coalescedPersistenceOps.TryAdd(compositeKey, op);
            if (!added)
                _coalescedPersistenceOps[compositeKey] = op;

            if (added)
                Interlocked.Increment(ref _coalescedDepth);

            if (Interlocked.Increment(ref _coalescedLogCounter) % 256 == 1)
            {
                _logger.LogWarning(
                    "Persistence queue {Reason} at {QueueDepth}/{QueueLimit}; coalescing latest state. CoalescedCount={CoalescedCount}",
                    reason, queueDepth, _maxPersistenceQueueSize, Volatile.Read(ref _coalescedDepth));
            }
        }

        // Background loop: waits for channel data or periodic timeout, then drains in one pass,
        // grouping operations by tenant so each tenant gets a single SQLite transaction
        // instead of one transaction per operation. This dramatically reduces SQLite overhead
        // under burst writes (e.g. 1000 concurrent file writes -> 1 transaction per tenant).
        private async Task RunPersistenceLoopAsync(CancellationToken ct = default)
        {
            int drainCycles = 0;
            while (true)
            {
                try
                {
                    await WaitForPersistenceWorkAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    break; // shutdown requested - fall through to drain
                }

                // Wrap the entire per-cycle work in a broad catch so that an unexpected
                // exception from CompactPendingQueues or LogPerformanceSnapshotIfNeeded
                // cannot kill the background persistence thread and silently stop all future
                // SQLite writes.  DrainPersistenceQueue already catches internally, but
                // this guard covers the outer cycle as a final safety net.
                try
                {
                    DrainPersistenceQueue();

                    // Periodically compact pending queues to reclaim memory occupied by stale
                    // entries left over from completed/failed files that were never explicitly removed.
                    if (++drainCycles % CompactionIntervalCycles == 0)
                        CompactPendingQueues();

                    LogPerformanceSnapshotIfNeeded(drainCycles);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex,
                        "Unexpected error in persistence drain cycle {Cycle}; loop will continue",
                        drainCycles);
                }
            }

            // Drain any items enqueued before cancellation was observed.
            var remainingQueued = Volatile.Read(ref _persistenceChannelDepth);
            var remainingCoalesced = Volatile.Read(ref _coalescedDepth);
            _logger.LogInformation(
                "Persistence loop exited, starting final drain (QueueDepth={QueueDepth}, CoalescedDepth={CoalescedDepth})",
                remainingQueued, remainingCoalesced);

            DrainPersistenceQueue();

            var afterQueued = Volatile.Read(ref _persistenceChannelDepth);
            var afterCoalesced = Volatile.Read(ref _coalescedDepth);
            _logger.LogInformation(
                "Final drain completed (QueueDepth={QueueDepth}, CoalescedDepth={CoalescedDepth})",
                afterQueued, afterCoalesced);
        }

        private void LogPerformanceSnapshotIfNeeded(int drainCycles)
        {
            if (drainCycles % 128 != 0)
                return;

            var pendingAttempts = Interlocked.Read(ref _pendingDequeuedAttempts);
            var pendingStaleSkips = Interlocked.Read(ref _pendingDequeuedStaleSkips);
            var staleRatio = pendingAttempts > 0
                ? pendingStaleSkips / (double)pendingAttempts
                : 0;

            _logger.LogInformation(
                "MetadataRepository performance counters: QueueDepth={QueueDepth}, QueuePeakDepth={QueuePeakDepth}, CoalescedDepth={CoalescedDepth}, PendingStaleSkipRatio={PendingStaleSkipRatio:P2}, StatusIndexStaleSkips={StatusIndexStaleSkips}",
                Volatile.Read(ref _persistenceChannelDepth),
                Volatile.Read(ref _persistenceChannelPeakDepth),
                Volatile.Read(ref _coalescedDepth),
                staleRatio,
                Interlocked.Read(ref _statusIndexStaleSkips));
        }

        private async Task WaitForPersistenceWorkAsync(CancellationToken ct = default)
        {
            // Periodic timeout keeps coalesced-only writes moving even when no new channel items arrive.
            var waitToReadTask = _persistenceReader.WaitToReadAsync(ct).AsTask();
            var timeoutTask = Task.Delay(TimeSpan.FromSeconds(_persistenceIntervalSeconds), ct);
            var completed = await Task.WhenAny(waitToReadTask, timeoutTask).ConfigureAwait(false);
            if (ct.IsCancellationRequested)
                throw new OperationCanceledException(ct);

            if (completed == waitToReadTask && !await waitToReadTask.ConfigureAwait(false))
                throw new OperationCanceledException(ct);
        }

        // Drains queued and coalesced operations in bounded batches.
        // Operations are grouped by tenantId and flushed in a single SQLite transaction
        // per tenant, minimising fsync overhead and memory spikes.
        private void DrainPersistenceQueue()
        {
            while (DrainPersistenceBatch())
            {
                // Keep draining until both queues are empty.
            }
        }

        private bool DrainPersistenceBatch()
        {
            var byTenant = new Dictionary<string, List<PersistenceOperation>>(StringComparer.Ordinal);
            int drainedCount = 0;

            // Use IsEmpty (O(1)) rather than _coalescedDepth > 0 to decide whether coalesced
            // entries need draining.  _coalescedDepth is a best-effort monitoring counter: a
            // race between CoalescePersistenceOperation and the drain's TryRemove can leave the
            // counter at 0 while an entry still exists in the map.  Falling back to IsEmpty
            // eliminates this blind-spot: if any entry is present it will always be found.
            var hasCoalesced = !_coalescedPersistenceOps.IsEmpty;
            var maxQueuedThisBatch = hasCoalesced && _maxDrainBatchSize > 1
                ? _maxDrainBatchSize - 1
                : _maxDrainBatchSize;

            while (drainedCount < maxQueuedThisBatch && _persistenceReader.TryRead(out var queuedOp))
            {
                drainedCount++;
                Interlocked.Decrement(ref _persistenceChannelDepth);
                AddOperationToTenantBucket(byTenant, queuedOp);
            }

            if (drainedCount < _maxDrainBatchSize && !_coalescedPersistenceOps.IsEmpty)
            {
                foreach (var coalesced in _coalescedPersistenceOps)
                {
                    if (drainedCount >= _maxDrainBatchSize)
                        break;

                    if (!_coalescedPersistenceOps.TryRemove(coalesced.Key, out var coalescedOp))
                        continue;

                    drainedCount++;
                    Interlocked.Decrement(ref _coalescedDepth);
                    AddOperationToTenantBucket(byTenant, coalescedOp);
                }
            }

            if (drainedCount == 0)
                return false;

            var encounteredFailure = false;
            foreach (var kvp in byTenant)
            {
                if (!ExecuteBatch(kvp.Key, kvp.Value))
                    encounteredFailure = true;
            }

            _logger.LogDebug(
                "Flushed persistence batch: {Drained} operations, {TenantBuckets} tenant bucket(s), RemainingQueue={QueueDepth}, RemainingCoalesced={CoalescedDepth}",
                drainedCount,
                byTenant.Count,
                Volatile.Read(ref _persistenceChannelDepth),
                Volatile.Read(ref _coalescedDepth));

            return !encounteredFailure;
        }

        private static void AddOperationToTenantBucket(
            Dictionary<string, List<PersistenceOperation>> byTenant,
            PersistenceOperation op)
        {
            if (!byTenant.TryGetValue(op.TenantId, out var bucket))
            {
                bucket = new List<PersistenceOperation>();
                byTenant[op.TenantId] = bucket;
            }

            bucket.Add(op);
        }

        private void ExecuteDirectBatch(string tenantId, IReadOnlyList<PersistenceOperation> ops)
        {
            if (ops == null)
                throw new ArgumentNullException(nameof(ops));

            if (ops.Count == 0)
                return;

            ExecuteBatchCore(tenantId, ops);
        }

        private void ExecuteBatchCore(string tenantId, IReadOnlyList<PersistenceOperation> ops)
        {
            lock (GetDatabaseLock(tenantId))
            {
                var conn = GetDatabase(tenantId);

                using var txn = conn.BeginTransaction();
                try
                {
                    foreach (var op in ops)
                    {
                        if (op.IsDelete)
                            DeleteFile(conn, txn, op.FileKey);
                        else
                            UpsertFile(conn, txn, op.Metadata!);
                    }

                    txn.Commit();

                    if (_sqliteOptions.CheckpointAfterBatch)
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = "PRAGMA wal_checkpoint(PASSIVE);";
                        cmd.ExecuteNonQuery();
                    }

                    _logger.LogDebug("Flushed {Count} persistence operations for tenant {TenantId}", ops.Count, tenantId);
                }
                catch
                {
                    txn.Rollback();
                    throw;
                }
            }
        }

        // Writes a batch of operations for a single tenant inside one SQLite transaction.
        // Errors are logged but never propagated -- SQLite unavailability must never fail writes.
        private bool ExecuteBatch(string tenantId, List<PersistenceOperation> ops)
        {
            try
            {
                ExecuteBatchCore(tenantId, ops);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "SQLite batch write failed for tenant {TenantId} ({Count} operations). " +
                    "Physical files are safe on disk; metadata may be lost if process restarts before SQLite recovers.",
                    tenantId, ops.Count);

                RequeueFailedPersistenceBatch(ops);
                return false;
            }
        }

        private void RequeueFailedPersistenceBatch(List<PersistenceOperation> ops)
        {
            if (!_enableBackgroundPersistence || _disposed || ops.Count == 0)
                return;

            var coalescedAdded = 0;
            var coalescedSkipped = 0;

            foreach (var op in ops)
            {
                var compositeKey = $"{op.TenantId}\u001F{op.FileKey}";
                if (_coalescedPersistenceOps.TryAdd(compositeKey, op))
                {
                    Interlocked.Increment(ref _coalescedDepth);
                    coalescedAdded++;
                }
                else
                {
                    // Keep the already coalesced latest state if one exists.
                    coalescedSkipped++;
                }
            }

            if (coalescedAdded > 0)
            {
                _logger.LogWarning(
                    "Requeued failed SQLite batch by coalescing {CoalescedAdded} operation(s) (skipped {CoalescedSkipped} newer coalesced state(s)). QueueDepth={QueueDepth}, CoalescedDepth={CoalescedDepth}",
                    coalescedAdded,
                    coalescedSkipped,
                    Volatile.Read(ref _persistenceChannelDepth),
                    Volatile.Read(ref _coalescedDepth));
            }
            else if (coalescedSkipped > 0)
            {
                _logger.LogDebug(
                    "Skipped requeue for {CoalescedSkipped} failed SQLite operation(s) because newer coalesced state already exists.",
                    coalescedSkipped);
            }
        }

        private static void UpsertFile(SqliteConnection conn, FileMetadata m)
        {
            using var txn = conn.BeginTransaction();
            UpsertFile(conn, txn, m);
            txn.Commit();
        }

        private static void UpsertFile(SqliteConnection conn, SqliteTransaction txn, FileMetadata m)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = @"
INSERT INTO files (
    file_key, tenant_id, volume_id, physical_path, directory_path, file_size,
    created_at, status, retry_count, last_failed_at, last_error,
    processing_start_time, completed_at, delete_succeeded_at, dead_lettered_at, available_for_processing_at,
    original_file_name, file_extension, metadata_json)
VALUES (
    @file_key, @tenant_id, @volume_id, @physical_path, @directory_path, @file_size,
    @created_at, @status, @retry_count, @last_failed_at, @last_error,
    @processing_start_time, @completed_at, @delete_succeeded_at, @dead_lettered_at, @available_for_processing_at,
    @original_file_name, @file_extension, @metadata_json)
ON CONFLICT(file_key) DO UPDATE SET
    tenant_id                  = excluded.tenant_id,
    volume_id                  = excluded.volume_id,
    physical_path              = excluded.physical_path,
    directory_path             = excluded.directory_path,
    file_size                  = excluded.file_size,
    created_at                 = excluded.created_at,
    status                     = excluded.status,
    retry_count                = excluded.retry_count,
    last_failed_at             = excluded.last_failed_at,
    last_error                 = excluded.last_error,
    processing_start_time      = excluded.processing_start_time,
    completed_at               = excluded.completed_at,
    delete_succeeded_at        = excluded.delete_succeeded_at,
    dead_lettered_at           = excluded.dead_lettered_at,
    available_for_processing_at= excluded.available_for_processing_at,
    original_file_name         = excluded.original_file_name,
    file_extension             = excluded.file_extension,
    metadata_json              = excluded.metadata_json;";

            cmd.Parameters.AddWithValue("@file_key",                   m.FileKey);
            cmd.Parameters.AddWithValue("@tenant_id",                  m.TenantId);
            cmd.Parameters.AddWithValue("@volume_id",                  m.VolumeId);
            cmd.Parameters.AddWithValue("@physical_path",              m.PhysicalPath);
            cmd.Parameters.AddWithValue("@directory_path",             m.DirectoryPath);
            cmd.Parameters.AddWithValue("@file_size",                  m.FileSize);
            cmd.Parameters.AddWithValue("@created_at",                 m.CreatedAt.ToString("O"));
            cmd.Parameters.AddWithValue("@status",                     (int)m.Status);
            cmd.Parameters.AddWithValue("@retry_count",                m.RetryCount);
            cmd.Parameters.AddWithValue("@last_failed_at",             m.LastFailedAt.HasValue ? (object)m.LastFailedAt.Value.ToString("O") : DBNull.Value);
            cmd.Parameters.AddWithValue("@last_error",                 (object?)m.LastError ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@processing_start_time",      m.ProcessingStartTime.HasValue ? (object)m.ProcessingStartTime.Value.ToString("O") : DBNull.Value);
            cmd.Parameters.AddWithValue("@completed_at",               m.CompletedAt.HasValue ? (object)m.CompletedAt.Value.ToString("O") : DBNull.Value);
            cmd.Parameters.AddWithValue("@delete_succeeded_at",        m.DeleteSucceededAt.HasValue ? (object)m.DeleteSucceededAt.Value.ToString("O") : DBNull.Value);
            cmd.Parameters.AddWithValue("@dead_lettered_at",           m.DeadLetteredAt.HasValue ? (object)m.DeadLetteredAt.Value.ToString("O") : DBNull.Value);
            cmd.Parameters.AddWithValue("@available_for_processing_at",m.AvailableForProcessingAt.HasValue ? (object)m.AvailableForProcessingAt.Value.ToString("O") : DBNull.Value);
            cmd.Parameters.AddWithValue("@original_file_name",         (object?)m.OriginalFileName ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@file_extension",             (object?)m.FileExtension ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@metadata_json",              m.Metadata != null ? (object)System.Text.Json.JsonSerializer.Serialize(m.Metadata) : DBNull.Value);
            cmd.ExecuteNonQuery();
        }

        private static void DeleteFile(SqliteConnection conn, SqliteTransaction txn, string fileKey)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = "DELETE FROM files WHERE file_key = @file_key;";
            cmd.Parameters.AddWithValue("@file_key", fileKey);
            cmd.ExecuteNonQuery();
        }

        // Rebuilds _pendingKeys / _delayedQueues for any tenant whose queues are significantly larger than
        // the number of actual Pending files in the cache.  Called periodically by the
        // single background persistence thread - never from a request path.
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
                CompactPendingQueueForTenant(tenantId, cache, force: false);
            }
        }

        private void CompactPendingQueueForTenant(
            string tenantId,
            ConcurrentDictionary<string, FileMetadata> cache,
            bool force)
        {
            if (!_pendingKeys.TryGetValue(tenantId, out var queue))
                return;

            // O(1): read from the counter maintained by AddOrUpdateAsync / RemoveAsync.
            _pendingFileCounts.TryGetValue(tenantId, out var actualPendingCount);
            var delayedQueueCount = _delayedQueues.TryGetValue(tenantId, out var delayedQueue)
                ? delayedQueue.Count
                : 0;

            if (!force)
            {
                // Only rebuild when the queue is significantly bloated:
                // more than 2x actual pending entries + a 100-entry slack buffer.
                if (queue.Count <= actualPendingCount * 2 + 100
                    && delayedQueueCount <= actualPendingCount * 2 + 100)
                    return;
            }

            _logger.LogDebug(
                "Compacting queues for tenant {TenantId}: Ready={ReadyCount}, Delayed={DelayedCount} -> Pending={PendingCount}, Force={Force}",
                tenantId, queue.Count, delayedQueueCount, actualPendingCount, force);

            var now = DateTime.UtcNow;
            var pendingItems = cache.Values
                .Where(m => m.Status == FileProcessingStatus.Pending)
                .OrderBy(m => m.CreatedAt)
                .ToList();

            // Build the new generations map as a fresh dictionary so that concurrent
            // IncrementPendingGeneration calls (from AddOrUpdateAsync on other threads) cannot
            // lose their writes into a window opened by generations.Clear().
            // Using TryUpdate to atomically replace the reference mirrors the same pattern
            // used for _pendingKeys below, keeping the race window as narrow as possible.
            var existingGenerations = _pendingGenerations.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, long>(StringComparer.Ordinal));

            var readyEntries = new List<PendingQueueEntry>(pendingItems.Count);
            var delayedItems = new List<FileMetadata>(pendingItems.Count);
            var newGenerations = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);

            foreach (var item in pendingItems)
            {
                newGenerations[item.FileKey] = 1;
                var entry = new PendingQueueEntry(item.FileKey, 1);
                if (item.AvailableForProcessingAt.HasValue && item.AvailableForProcessingAt.Value > now)
                    delayedItems.Add(item);
                else
                    readyEntries.Add(entry);
            }

            // Atomically replace both the generations map and the pending queue so that
            // concurrent callers that already hold a reference to the old instances
            // continue to operate correctly (their entries are validated on dequeue).
            _pendingGenerations.TryUpdate(tenantId, newGenerations, existingGenerations);

            // TryUpdate atomically replaces the queue only if _pendingKeys[tenantId] still
            // points to the same instance we observed earlier (i.e., `queue`).  This narrows
            // the race window: if a concurrent AddOrUpdateAsync already swapped in a newer
            // queue, we leave it untouched rather than silently discarding newly enqueued keys.
            var newQueue = new ConcurrentQueue<PendingQueueEntry>(readyEntries);
            _pendingKeys.TryUpdate(tenantId, newQueue, queue);

            if (delayedItems.Count == 0)
            {
                if (delayedQueue != null)
                    _delayedQueues.TryRemove(tenantId, out _);
                return;
            }

            delayedItems = delayedItems
                .OrderBy(m => m.AvailableForProcessingAt)
                .ThenBy(m => m.CreatedAt)
                .ToList();

            var newDelayed = new DelayedQueue();
            var endSequence = Interlocked.Add(ref _delayedSequence, delayedItems.Count);
            var sequence = endSequence - delayedItems.Count;
            foreach (var item in delayedItems)
            {
                newDelayed.Enqueue(item.FileKey, 1, item.AvailableForProcessingAt!.Value, ++sequence);
            }

            if (delayedQueue != null)
                _delayedQueues.TryUpdate(tenantId, newDelayed, delayedQueue);
            else
                _delayedQueues.TryAdd(tenantId, newDelayed);
        }

        /// <summary>
        /// Disposes the repository and closes all SQLite connections.
        /// Signals the write-behind background task to stop, waits up to
        /// <see cref="_shutdownDrainTimeoutSeconds"/> for it to drain remaining
        /// queued writes, then closes all SQLite connections.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_enableBackgroundPersistence)
            {
                var queueDepthBefore = Volatile.Read(ref _persistenceChannelDepth);
                var coalescedBefore = Volatile.Read(ref _coalescedDepth);
                _logger.LogInformation(
                    "MetadataRepository shutting down: draining persistence queue (QueueDepth={QueueDepth}, CoalescedDepth={CoalescedDepth}, Timeout={TimeoutSeconds}s)",
                    queueDepthBefore, coalescedBefore, _shutdownDrainTimeoutSeconds);

                // 1. Complete the channel writer so WaitToReadAsync returns false gracefully,
                // allowing the persistence loop to exit without OperationCanceledException
                // and proceed to the final DrainPersistenceQueue() call.
                _persistenceWriter.Complete();

                // 2. Cancel as fallback safety net in case the loop is stuck elsewhere.
                _persistenceCts.Cancel();

                // 3. Wait for the background task to drain remaining queued items.
                // On K8s graceful shutdown (SIGTERM), this window allows in-flight writes to reach SQLite.
                // If SQLite is unavailable or the queue is too large to drain in time, remaining items
                // are discarded -- physical files are safe on disk and recovered by OrphanFileRecoveryService on restart.
                try
                {
                    _persistenceTask.Wait(TimeSpan.FromSeconds(_shutdownDrainTimeoutSeconds));
                }
                catch (AggregateException)
                {
                    // OperationCanceledException wrapped in AggregateException -- expected on cancellation
                }

                var queueDepthAfter = Volatile.Read(ref _persistenceChannelDepth);
                var coalescedAfter = Volatile.Read(ref _coalescedDepth);
                if (queueDepthAfter == 0 && coalescedAfter == 0)
                {
                    _logger.LogInformation("MetadataRepository shutdown drain completed successfully �� all queued writes persisted to SQLite");
                }
                else
                {
                    _logger.LogWarning(
                        "MetadataRepository shutdown drain incomplete: {QueueRemaining} queued + {CoalescedRemaining} coalesced operations were NOT persisted. " +
                        "These files will be recovered as orphans on next startup by OrphanFileRecoveryService",
                        queueDepthAfter, coalescedAfter);
                }
            }

            // 4. Close all connections AFTER the drain is complete
            foreach (var lazyConn in _databases.Values)
            {
                try
                {
                    if (lazyConn.IsValueCreated)
                        lazyConn.Value?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing SQLite connection");
                }
            }

            _databases.Clear();
            _activeFiles.Clear();
            _fileKeyTenantIndex.Clear();
            _physicalPathIndex.Clear();
            _pendingKeys.Clear();
            _pendingGenerations.Clear();
            _prefetchedPendingByTenant.Clear();
            _pendingFileCounts.Clear();
            _processingByStartTime.Clear();
            _processingByStartTimeLocks.Clear();
            _completedByCompletedAt.Clear();
            _completedByCompletedAtLocks.Clear();
            _deleteRequestedByCompletedAt.Clear();
            _deleteRequestedByCompletedAtLocks.Clear();
            _permanentlyFailedByLastFailedAt.Clear();
            _permanentlyFailedByLastFailedAtLocks.Clear();
            _delayedQueues.Clear();
            _delayedQueueLocks.Clear();
            _coalescedPersistenceOps.Clear();
            _databaseLocks.Clear();

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
            _persistenceCts.Dispose();

            _logger.LogInformation("MetadataRepository disposed");
        }
    }
}


