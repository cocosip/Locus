using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Locus.Core.Models;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Repository for directory quota storage using per-tenant SQLite with in-memory caching.
    /// Hot path (TryIncrementAsync / DecrementAsync) is lock-free using CAS atomic counters.
    /// SQLite writes are deferred via a Write-Behind timer (every 5 seconds) for performance.
    /// Thread-safe for concurrent access.
    /// </summary>
    public class DirectoryQuotaRepository : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DirectoryQuotaRepository> _logger;
        private readonly string _quotaDirectory;
        private readonly SqliteOptions _sqliteOptions;

        // Per-tenant in-memory cache (source of truth for MaxCount / Enabled config)
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>> _quotaCache;

        // Per-tenant SQLite connections (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<SqliteConnection>> _databases;
        private readonly ConcurrentDictionary<string, object> _databaseLocks;

        // Per-tenant operation locks (used for config operations; NOT used in hot path)
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;
        private static readonly AsyncLocal<HashSet<string>> _tenantLockBypass = new AsyncLocal<HashSet<string>>();

        // --- Write-Behind: atomic counters + background flush timer ---

        /// <summary>
        /// Per-tenant atomic counter state for lock-free hot path.
        /// Count is the authoritative live value; SQLite is updated asynchronously.
        /// </summary>
        private sealed class AtomicQuotaState
        {
            /// <summary>Live file count. Use Interlocked operations only.</summary>
            public int Count;

            /// <summary>Maximum allowed count (0 = unlimited). Updated by UpdateAsync.</summary>
            public volatile int MaxCount;

            /// <summary>Whether quota enforcement is active. Updated by UpdateAsync.</summary>
            public volatile bool Enabled;

            /// <summary>True when Count has changed since last SQLite flush.</summary>
            public volatile bool Dirty;
        }

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, AtomicQuotaState>> _atomicCounters;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _dirtyDirectoryKeysByTenant;
        private readonly ConcurrentDictionary<string, byte> _dirtyTenants;
        private readonly Timer? _flushTimer;
        private readonly bool _enableBackgroundFlush;
        private const int FlushIntervalMs = 5_000; // 5 seconds

        // ---

        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaRepository"/> class.
        /// </summary>
        public DirectoryQuotaRepository(
            IFileSystem fileSystem,
            ILogger<DirectoryQuotaRepository> logger,
            string quotaDirectory,
            SqliteOptions? sqliteOptions = null,
            bool enableBackgroundFlush = true)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(quotaDirectory))
                throw new ArgumentException("Quota directory cannot be empty", nameof(quotaDirectory));

            _quotaDirectory = quotaDirectory;
            _sqliteOptions = sqliteOptions ?? new SqliteOptions();
            _enableBackgroundFlush = enableBackgroundFlush;
            _quotaCache = new ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>>();
            _databases = new ConcurrentDictionary<string, Lazy<SqliteConnection>>();
            _databaseLocks = new ConcurrentDictionary<string, object>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _atomicCounters = new ConcurrentDictionary<string, ConcurrentDictionary<string, AtomicQuotaState>>();
            _dirtyDirectoryKeysByTenant = new ConcurrentDictionary<string, ConcurrentDictionary<string, byte>>();
            _dirtyTenants = new ConcurrentDictionary<string, byte>();

            // Ensure quota directory exists
            if (!_fileSystem.Directory.Exists(_quotaDirectory))
            {
                _fileSystem.Directory.CreateDirectory(_quotaDirectory);
            }

            _logger.LogInformation(
                "DirectoryQuotaRepository initialized at {Directory} with SQLite: JournalMode={JournalMode}, SynchronousMode={SynchronousMode}, BusyTimeout={BusyTimeout}ms",
                _quotaDirectory,
                _sqliteOptions.JournalMode,
                _sqliteOptions.SynchronousMode,
                _sqliteOptions.BusyTimeoutMs);

            if (_enableBackgroundFlush)
            {
                // Start the Write-Behind flush timer (non-reentrant: fires once then reschedules)
                _flushTimer = new Timer(FlushDirtyCounters, null, FlushIntervalMs, Timeout.Infinite);
            }
            else
            {
                _logger.LogInformation("DirectoryQuotaRepository background flush timer is disabled.");
            }
        }

        private const string QuotaDdl = @"
CREATE TABLE IF NOT EXISTS quotas (
    directory_path TEXT PRIMARY KEY NOT NULL,
    current_count  INTEGER NOT NULL DEFAULT 0,
    max_count      INTEGER NOT NULL DEFAULT 0,
    enabled        INTEGER NOT NULL DEFAULT 1,
    last_updated   TEXT NOT NULL,
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quotas_enabled ON quotas(enabled);";

        /// <summary>
        /// Gets or creates a SQLite connection for a tenant.
        /// </summary>
        private SqliteConnection GetDatabase(string tenantId)
        {
            // Use Lazy<SqliteConnection> to ensure thread-safe initialization.
            var lazyConn = _databases.GetOrAdd(tenantId, tid => new Lazy<SqliteConnection>(() =>
            {
                try
                {
                    // Ensure tenant subdirectory exists (thread-safe in case of concurrent access)
                    var tenantDir = _fileSystem.Path.Combine(_quotaDirectory, tid);
                    if (!_fileSystem.Directory.Exists(tenantDir))
                        _fileSystem.Directory.CreateDirectory(tenantDir);

                    var dbPath = _fileSystem.Path.Combine(tenantDir, "quotas.db");
                    var conn = OpenAndInitializeConnection(dbPath);

                    _logger.LogDebug("Created/opened SQLite quota database for tenant {TenantId} at {Path}", tid, dbPath);

                    // Load all quotas into memory on first access
                    LoadQuotasForTenant(tid, conn);

                    return conn;
                }
                catch (Exception ex) when (IsRecoverableDatabaseException(ex))
                {
                    _logger.LogError(ex,
                        "CORRUPTED QUOTA DATABASE DETECTED for tenant {TenantId} during initialization. Attempting automatic recovery...",
                        tid);

                    try
                    {
                        // IMPORTANT: Do NOT call _databases.TryRemove(tid) or RebuildDatabaseNoLock(tid)
                        // from inside the Lazy factory:
                        //   - TryRemove would allow a concurrent thread to insert a competing Lazy
                        //     into _databases while this factory is still executing, creating a race
                        //     where two threads initialize the same database file simultaneously.
                        //   - RebuildDatabaseNoLock calls lazyConn.Value which would deadlock because
                        //     the Lazy lock (ExecutionAndPublication) is already held by this thread.
                        // Instead, perform the file-level rebuild operations inline here.
                        // The Lazy lock itself guarantees that only this thread is running this path.
                        var dbPath = _fileSystem.Path.Combine(_quotaDirectory, tid, "quotas.db");

                        // Clear stale in-memory state so the Write-Behind timer cannot flush
                        // old counts into the freshly rebuilt database.
                        _quotaCache.TryRemove(tid, out _);
                        _atomicCounters.TryRemove(tid, out _);
                        _dirtyDirectoryKeysByTenant.TryRemove(tid, out _);
                        _dirtyTenants.TryRemove(tid, out _);

                        if (_fileSystem.File.Exists(dbPath))
                        {
                            // Release OS file handles before deleting (required on Windows).
                            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();

                            var backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
                            _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
                            _logger.LogInformation("Backed up corrupted quota database: {BackupPath}", backupPath);

                            _fileSystem.File.Delete(dbPath);
                            _logger.LogInformation("Deleted corrupted quota database: {DatabasePath}", dbPath);

                            DeleteSidecarFiles(dbPath);
                        }

                        // Open fresh connection and reload quotas into the in-memory cache.
                        var recoveredConn = OpenAndInitializeConnection(dbPath);
                        LoadQuotasForTenant(tid, recoveredConn);

                        _logger.LogWarning(
                            "Quota database rebuilt for tenant {TenantId} during initialization.",
                            tid);
                        _logger.LogInformation(
                            "Successfully recovered and initialized quota database for tenant {TenantId}",
                            tid);

                        return recoveredConn;
                    }
                    catch (Exception recoveryEx)
                    {
                        _logger.LogError(recoveryEx,
                            "Failed to recover corrupted quota database for tenant {TenantId} during initialization",
                            tid);
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

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = _sqliteOptions.BuildPragmaSql();
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = QuotaDdl;
                cmd.ExecuteNonQuery();
            }

            return conn;
        }

        /// <summary>
        /// Loads all quotas into memory for a tenant.
        /// </summary>
        private void LoadQuotasForTenant(string tenantId, SqliteConnection conn)
        {
            // buffered: false streams rows directly into the cache loop instead of loading
            // the full result set into a List<T> first, reducing peak memory for tenants
            // with many quota entries.
            var rows = conn.Query<QuotaRow>("SELECT * FROM quotas", buffered: false);
            var count = 0;
            var cache = _quotaCache.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, DirectoryQuota>());
            foreach (var row in rows)
            {
                var quota = row.ToDirectoryQuota();
                cache[quota.DirectoryPath] = quota;
                count++;
            }

            if (count > 0)
                _logger.LogInformation("Loaded {Count} directory quotas for tenant {TenantId} into memory", count, tenantId);
        }

        /// <summary>
        /// Gets the in-memory cache for a tenant.
        /// </summary>
        private ConcurrentDictionary<string, DirectoryQuota> GetCache(string tenantId)
        {
            // Ensure database is initialized (which also loads quotas)
            GetDatabase(tenantId);
            return _quotaCache.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, DirectoryQuota>());
        }

        /// <summary>
        /// Gets or creates a tenant-specific lock shared by config operations.
        /// NOT used in the hot path (TryIncrementAsync / DecrementAsync).
        /// </summary>
        private SemaphoreSlim GetTenantLock(string tenantId)
        {
            return _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
        }

        /// <summary>
        /// Acquires tenant lock unless current async flow already owns a rebuild lock for this tenant.
        /// </summary>
        private async Task<SemaphoreSlim?> AcquireTenantLockIfNeededAsync(string tenantId, CancellationToken ct)
        {
            if (_tenantLockBypass.Value != null && _tenantLockBypass.Value.Contains(tenantId))
                return null;

            var tenantLock = GetTenantLock(tenantId);
            await tenantLock.WaitAsync(ct);
            return tenantLock;
        }

        /// <summary>
        /// Registers tenant lock bypass for current async flow.
        /// </summary>
        private static void AddTenantLockBypass(string tenantId)
        {
            var bypass = _tenantLockBypass.Value;
            if (bypass == null)
            {
                bypass = new HashSet<string>(StringComparer.Ordinal);
                _tenantLockBypass.Value = bypass;
            }

            bypass.Add(tenantId);
        }

        /// <summary>
        /// Removes tenant lock bypass for current async flow.
        /// </summary>
        private static void RemoveTenantLockBypass(string tenantId)
        {
            _tenantLockBypass.Value?.Remove(tenantId);
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
        /// Checks whether the exception indicates a recoverable database corruption scenario.
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
        /// Rebuilds a tenant quota database in-place.
        /// Must be called while holding the tenant lock.
        /// </summary>
        private string? RebuildDatabaseNoLock(string tenantId)
        {
            var dbPath = _fileSystem.Path.Combine(_quotaDirectory, tenantId, "quotas.db");

            if (!_fileSystem.File.Exists(dbPath))
            {
                _logger.LogInformation("No quota database file to rebuild for tenant {TenantId}", tenantId);
                return null;
            }

            lock (GetDatabaseLock(tenantId))
            {
                // Remove from cache FIRST so no other thread can acquire the disposed instance.
                _databases.TryRemove(tenantId, out var lazyConn);

                // Dispose the removed connection (safe: it's no longer reachable via cache).
                if (lazyConn != null && lazyConn.IsValueCreated)
                {
                    try
                    {
                        lazyConn.Value?.Dispose();
                        _logger.LogDebug("Disposed SQLite quota connection for rebuild: {TenantId}", tenantId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Error disposing SQLite quota connection for tenant {TenantId}", tenantId);
                    }
                }

                // Force the connection pool to release the OS file handle before deleting the file.
                // NOTE: ClearAllPools() is a global operation that affects ALL tenant connections.
                // This is intentional and necessary: the OS file handle must be released before the
                // database file can be deleted. Since database rebuild is a rare maintenance operation
                // (only triggered by corruption), the brief impact on other tenants is acceptable.
                Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
                _quotaCache.TryRemove(tenantId, out _);

                // Clear atomic counters and dirty tracking so stale in-memory state cannot be
                // flushed back into the freshly rebuilt database by the Write-Behind timer.
                // Without this, the flush timer would re-populate the new DB with old counts
                // for directories that may no longer exist after the rebuild.
                if (_atomicCounters.TryRemove(tenantId, out _))
                    _logger.LogDebug("Cleared atomic counters for tenant {TenantId} during rebuild", tenantId);
                if (_dirtyDirectoryKeysByTenant.TryRemove(tenantId, out _))
                    _logger.LogDebug("Cleared dirty directory keys for tenant {TenantId} during rebuild", tenantId);
                _dirtyTenants.TryRemove(tenantId, out _);

                // Backup and delete corrupted database
                var backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
                _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
                _logger.LogInformation("Backed up corrupted quota database: {BackupPath}", backupPath);

                _fileSystem.File.Delete(dbPath);
                _logger.LogInformation("Deleted corrupted quota database: {DatabasePath}", dbPath);

                // Also delete SQLite WAL and SHM sidecar files so the new database starts clean.
                DeleteSidecarFiles(dbPath);

                return backupPath;
            }
        }

        /// <summary>
        /// Persists a quota change with automatic recovery on corruption.
        /// Must be called while holding the tenant lock.
        /// </summary>
        private void PersistQuotaWithRecoveryNoLock(string tenantId, DirectoryQuota quota, bool useUpsert)
        {
            try
            {
                lock (GetDatabaseLock(tenantId))
                {
                    var conn = GetDatabase(tenantId);
                    UpsertQuota(conn, quota);
                }

                var cache = GetCache(tenantId);
                cache[quota.DirectoryPath] = quota;
            }
            catch (Exception ex) when (IsRecoverableDatabaseException(ex))
            {
                _logger.LogError(ex,
                    "CORRUPTED QUOTA DATABASE DETECTED for tenant {TenantId}. Attempting automatic recovery...",
                    tenantId);

                try
                {
                    var backupPath = RebuildDatabaseNoLock(tenantId);

                    _logger.LogWarning(
                        "Quota database rebuilt for tenant {TenantId}. Backup saved at: {BackupPath}",
                        tenantId, backupPath ?? "N/A");

                    lock (GetDatabaseLock(tenantId))
                    {
                        var conn = GetDatabase(tenantId);
                        UpsertQuota(conn, quota);
                    }

                    var cache = GetCache(tenantId);
                    cache[quota.DirectoryPath] = quota;

                    _logger.LogInformation(
                        "Successfully recovered and persisted quota for tenant {TenantId}",
                        tenantId);
                }
                catch (Exception recoveryEx)
                {
                    _logger.LogError(
                        recoveryEx,
                        "Failed to recover corrupted quota database for tenant {TenantId}",
                        tenantId);
                    throw;
                }
            }
        }

        /// <summary>
        /// Gets an existing quota from cache or creates one if missing.
        /// Must be called while holding the tenant lock.
        /// </summary>
        private DirectoryQuota GetOrCreateNoLock(string tenantId, string directoryPath)
        {
            var cache = GetCache(tenantId);

            return cache.GetOrAdd(directoryPath, path =>
            {
                var newQuota = new DirectoryQuota
                {
                    DirectoryPath = path,
                    CurrentCount = 0,
                    MaxCount = 0, // 0 means no limit
                    Enabled = true,
                    CreatedAt = DateTime.UtcNow,
                    LastUpdated = DateTime.UtcNow
                };

                PersistQuotaWithRecoveryNoLock(tenantId, newQuota, useUpsert: true);
                return newQuota;
            });
        }

        // -----------------------------------------------------------------
        // Write-Behind: atomic counter state helpers
        // -----------------------------------------------------------------

        /// <summary>
        /// Gets or creates the <see cref="AtomicQuotaState"/> for a directory.
        /// Initializes Count / MaxCount / Enabled from the persisted cache on first access.
        /// This is lock-free and safe for concurrent calls.
        /// </summary>
        private AtomicQuotaState GetOrCreateAtomicState(string tenantId, string directoryPath)
        {
            // Ensure the database is open and _quotaCache is populated.
            // (No-op after first call per tenant due to Lazy.)
            GetDatabase(tenantId);

            var tenantCounters = _atomicCounters.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, AtomicQuotaState>());

            return tenantCounters.GetOrAdd(directoryPath, path =>
            {
                // Initialize from the persisted cache so restarts pick up the last flushed count.
                int initialCount = 0;
                int maxCount = 0;
                bool enabled = true;

                if (_quotaCache.TryGetValue(tenantId, out var tenantCache)
                    && tenantCache.TryGetValue(path, out var existing))
                {
                    initialCount = existing.CurrentCount;
                    maxCount = existing.MaxCount;
                    enabled = existing.Enabled;
                }

                return new AtomicQuotaState
                {
                    Count = initialCount,
                    MaxCount = maxCount,
                    Enabled = enabled,
                    Dirty = false
                };
            });
        }

        /// <summary>
        /// Returns a <see cref="DirectoryQuota"/> snapshot where CurrentCount reflects the
        /// live atomic count if available, falling back to the cached (possibly stale) count.
        /// MaxCount and Enabled are also taken from atomic state when present.
        /// </summary>
        private DirectoryQuota MergeWithLiveCount(string tenantId, DirectoryQuota quota)
        {
            if (_atomicCounters.TryGetValue(tenantId, out var tenantCounters)
                && tenantCounters.TryGetValue(quota.DirectoryPath, out var state))
            {
                return new DirectoryQuota
                {
                    DirectoryPath = quota.DirectoryPath,
                    CurrentCount = Volatile.Read(ref state.Count),
                    MaxCount = state.MaxCount,
                    Enabled = state.Enabled,
                    CreatedAt = quota.CreatedAt,
                    LastUpdated = quota.LastUpdated
                };
            }

            return quota;
        }

        // -----------------------------------------------------------------
        // Write-Behind: background flush timer
        // -----------------------------------------------------------------

        /// <summary>
        /// Marks a quota counter dirty and indexes it for targeted background flush.
        /// </summary>
        private void MarkDirty(string tenantId, string directoryPath, AtomicQuotaState state)
        {
            state.Dirty = true;

            var dirtyDirectories = _dirtyDirectoryKeysByTenant.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, byte>(StringComparer.Ordinal));
            dirtyDirectories[directoryPath] = 0;
            _dirtyTenants[tenantId] = 0;
        }

        /// <summary>
        /// Timer callback: flushes all dirty atomic counters to SQLite.
        /// Non-reentrant: uses Timeout.Infinite period and reschedules after completion.
        /// </summary>
        private void FlushDirtyCounters(object? state)
        {
            if (_disposed)
                return;

            try
            {
                DoFlushAllDirtyCounters();
            }
            finally
            {
                if (!_disposed)
                {
                    try
                    {
                        _flushTimer?.Change(FlushIntervalMs, Timeout.Infinite);
                    }
                    catch (ObjectDisposedException)
                    {
                        // Dispose can race with the timer callback during test/application shutdown.
                    }
                }
            }
        }

        /// <summary>
        /// Core flush logic: drains indexed dirty tenants/directories and writes them to SQLite.
        /// Safe to call from Dispose for a final drain.
        /// </summary>
        private void DoFlushAllDirtyCounters()
        {
            foreach (var tenantId in _dirtyTenants.Keys)
            {
                if (!_dirtyTenants.TryRemove(tenantId, out _))
                    continue;

                FlushDirtyCountersForTenant(tenantId);
            }
        }

        private void FlushDirtyCountersForTenant(string tenantId)
        {
            if (!_dirtyDirectoryKeysByTenant.TryGetValue(tenantId, out var dirtyDirectories))
                return;

            if (!_atomicCounters.TryGetValue(tenantId, out var tenantCounters))
                return;

            var cache = _quotaCache.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, DirectoryQuota>());
            var dirtySnapshots = new List<(string DirectoryPath, AtomicQuotaState State, DirectoryQuota Snapshot)>(dirtyDirectories.Count);
            var nowUtc = DateTime.UtcNow;

            foreach (var directoryPath in dirtyDirectories.Keys)
            {
                if (!dirtyDirectories.TryRemove(directoryPath, out _))
                    continue;

                if (!tenantCounters.TryGetValue(directoryPath, out var atomicState))
                    continue;

                if (!atomicState.Dirty)
                    continue;

                // Clear dirty BEFORE reading count so any concurrent increments that
                // happen during the SQLite write will mark Dirty=true again and be
                // captured in the next flush cycle.
                atomicState.Dirty = false;
                var count = Volatile.Read(ref atomicState.Count);

                // Build a new immutable snapshot so concurrent readers never see a
                // partially-updated object. Preserve CreatedAt from the existing cache
                // entry if one exists; fall back to nowUtc for brand-new directories.
                cache.TryGetValue(directoryPath, out var existing);
                var snapshot = new DirectoryQuota
                {
                    DirectoryPath = directoryPath,
                    CurrentCount  = count,
                    MaxCount      = atomicState.MaxCount,
                    Enabled       = atomicState.Enabled,
                    CreatedAt     = existing?.CreatedAt ?? nowUtc,
                    LastUpdated   = nowUtc
                };
                cache[directoryPath] = snapshot;
                dirtySnapshots.Add((directoryPath, atomicState, snapshot));
            }

            if (dirtySnapshots.Count == 0)
                return;

            try
            {
                lock (GetDatabaseLock(tenantId))
                {
                    var conn = GetDatabase(tenantId);

                    using var txn = conn.BeginTransaction();
                    try
                    {
                        foreach (var item in dirtySnapshots)
                            UpsertQuota(conn, txn, item.Snapshot);

                        txn.Commit();
                    }
                    catch
                    {
                        txn.Rollback();
                        throw;
                    }

                    if (_sqliteOptions.CheckpointAfterBatch)
                    {
                        using var cmd = conn.CreateCommand();
                        cmd.CommandText = "PRAGMA wal_checkpoint(PASSIVE);";
                        cmd.ExecuteNonQuery();
                    }
                }

                _logger.LogDebug(
                    "Flushed {Count} dirty quota counters for tenant {TenantId} in a single transaction",
                    dirtySnapshots.Count, tenantId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex,
                    "Failed to flush dirty quota counters for tenant {TenantId}; will retry on next flush (Count={Count})",
                    tenantId, dirtySnapshots.Count);

                foreach (var item in dirtySnapshots)
                {
                    // Re-mark dirty so the next flush retries.
                    MarkDirty(tenantId, item.DirectoryPath, item.State);
                }
            }
        }

        // -----------------------------------------------------------------
        // Public API
        // -----------------------------------------------------------------

        private static void UpsertQuota(SqliteConnection conn, DirectoryQuota q)
        {
            using var txn = conn.BeginTransaction();
            UpsertQuota(conn, txn, q);
            txn.Commit();
        }

        private static void UpsertQuota(SqliteConnection conn, SqliteTransaction txn, DirectoryQuota q)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = @"
INSERT INTO quotas (directory_path, current_count, max_count, enabled, last_updated, created_at)
VALUES (@dir, @cur, @max, @enabled, @updated, @created)
ON CONFLICT(directory_path) DO UPDATE SET
    current_count = excluded.current_count,
    max_count     = excluded.max_count,
    enabled       = excluded.enabled,
    last_updated  = excluded.last_updated;";
            cmd.Parameters.AddWithValue("@dir",     q.DirectoryPath);
            cmd.Parameters.AddWithValue("@cur",     q.CurrentCount);
            cmd.Parameters.AddWithValue("@max",     q.MaxCount);
            cmd.Parameters.AddWithValue("@enabled", q.Enabled ? 1 : 0);
            cmd.Parameters.AddWithValue("@updated", q.LastUpdated.ToString("O"));
            cmd.Parameters.AddWithValue("@created", q.CreatedAt.ToString("O"));
            cmd.ExecuteNonQuery();
        }

        private static void DeleteQuota(SqliteConnection conn, string directoryPath)
        {
            using var txn = conn.BeginTransaction();
            using var cmd = conn.CreateCommand();
            cmd.Transaction = txn;
            cmd.CommandText = "DELETE FROM quotas WHERE directory_path = @dir;";
            cmd.Parameters.AddWithValue("@dir", directoryPath);
            cmd.ExecuteNonQuery();
            txn.Commit();
        }

        // Dapper read DTO for the quotas table
        private sealed class QuotaRow
        {
            public string directory_path { get; set; } = string.Empty;
            public int    current_count  { get; set; }
            public int    max_count      { get; set; }
            public int    enabled        { get; set; }
            public string last_updated   { get; set; } = string.Empty;
            public string created_at     { get; set; } = string.Empty;

            public DirectoryQuota ToDirectoryQuota() => new DirectoryQuota
            {
                DirectoryPath = directory_path,
                CurrentCount  = current_count,
                MaxCount      = max_count,
                Enabled       = enabled != 0,
                LastUpdated   = ParseDateTime(last_updated),
                CreatedAt     = ParseDateTime(created_at)
            };

            private static DateTime ParseDateTime(string s)
            {
                if (DateTime.TryParse(s, null, System.Globalization.DateTimeStyles.RoundtripKind, out var result))
                    return result;

                // Fallback: handle non-ISO 8601 values written by older tooling or migration scripts.
                // If that also fails, return UTC epoch so the record stays loadable.
                if (DateTime.TryParse(s, out result))
                    return DateTime.SpecifyKind(result, DateTimeKind.Utc);

                return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            }
        }

        /// <summary>
        /// Gets or creates a directory quota.
        /// Returns a snapshot with the live current count from the atomic state.
        /// </summary>
        public async Task<DirectoryQuota> GetOrCreateAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                var quota = GetOrCreateNoLock(tenantId, directoryPath);
                return MergeWithLiveCount(tenantId, quota);
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Gets a directory quota by path.
        /// Returns a snapshot with the live current count from the atomic state.
        /// No lock needed: GetDatabase uses LazyThreadSafetyMode.ExecutionAndPublication
        /// and _quotaCache is a ConcurrentDictionary (lock-free reads).
        /// </summary>
        public Task<DirectoryQuota?> GetAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var cache = GetCache(tenantId);
            if (!cache.TryGetValue(directoryPath, out var quota))
                return Task.FromResult<DirectoryQuota?>(null);

            return Task.FromResult<DirectoryQuota?>(MergeWithLiveCount(tenantId, quota));
        }

        /// <summary>
        /// Updates a directory quota config (MaxCount, Enabled).
        /// Persists to SQLite immediately and syncs the atomic state so the hot path
        /// picks up the new limit without waiting for the next flush.
        /// </summary>
        public async Task UpdateAsync(string tenantId, DirectoryQuota quota, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (quota == null)
                throw new ArgumentNullException(nameof(quota));

            if (string.IsNullOrWhiteSpace(quota.DirectoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(quota));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                quota.LastUpdated = DateTime.UtcNow;
                PersistQuotaWithRecoveryNoLock(tenantId, quota, useUpsert: true);

                // Propagate config changes to atomic state so the hot-path CAS check
                // uses the updated limit immediately (without waiting for restart).
                if (_atomicCounters.TryGetValue(tenantId, out var tenantCounters)
                    && tenantCounters.TryGetValue(quota.DirectoryPath, out var state))
                {
                    state.MaxCount = quota.MaxCount;
                    state.Enabled = quota.Enabled;
                }

                _logger.LogDebug(
                    "Updated quota for directory: {DirectoryPath}, Tenant: {TenantId}, Current: {Current}, Max: {Max}",
                    quota.DirectoryPath, tenantId, quota.CurrentCount, quota.MaxCount);
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Sets the current file count for a directory and synchronizes atomic state immediately.
        /// Intended for maintenance workflows (e.g. database rebuild) where a deterministic count
        /// must take effect before returning.
        /// </summary>
        public async Task SetCurrentCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            if (count < 0)
                throw new ArgumentException("Count cannot be negative", nameof(count));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                SetCurrentCountNoLock(tenantId, directoryPath, count);
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Sets the current count without acquiring tenant lock.
        /// Callers must already hold the tenant lock for this tenant.
        /// </summary>
        internal Task SetCurrentCountForRebuildAsync(string tenantId, string directoryPath, int count, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            if (count < 0)
                throw new ArgumentException("Count cannot be negative", nameof(count));

            ct.ThrowIfCancellationRequested();
            SetCurrentCountNoLock(tenantId, directoryPath, count);
            return Task.CompletedTask;
        }

        private void SetCurrentCountNoLock(string tenantId, string directoryPath, int count)
        {
            var existing = GetOrCreateNoLock(tenantId, directoryPath);
            var snapshot = new DirectoryQuota
            {
                DirectoryPath = existing.DirectoryPath,
                CurrentCount = count,
                MaxCount = existing.MaxCount,
                Enabled = existing.Enabled,
                CreatedAt = existing.CreatedAt,
                LastUpdated = DateTime.UtcNow
            };

            PersistQuotaWithRecoveryNoLock(tenantId, snapshot, useUpsert: true);

            var state = GetOrCreateAtomicState(tenantId, directoryPath);
            Interlocked.Exchange(ref state.Count, count);
            state.MaxCount = snapshot.MaxCount;
            state.Enabled = snapshot.Enabled;
            state.Dirty = false;

            _logger.LogDebug(
                "Set current count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                directoryPath, tenantId, count, state.MaxCount);
        }

        /// <summary>
        /// Atomically increments the file count for a directory.
        /// Lock-free hot path: uses CAS (compare-and-swap) on the atomic counter.
        /// SQLite is updated asynchronously by the Write-Behind timer.
        /// Returns false if the increment would exceed the configured maximum count.
        /// </summary>
        public Task<bool> TryIncrementAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var state = GetOrCreateAtomicState(tenantId, directoryPath);

            // Fast-path: no limit configured or quota disabled — always allow.
            if (!state.Enabled || state.MaxCount == 0)
            {
                Interlocked.Increment(ref state.Count);
                MarkDirty(tenantId, directoryPath, state);
                return Task.FromResult(true);
            }

            // CAS check-and-increment: ensures no two threads can both read the same
            // count and both succeed when the count is already at the limit.
            SpinWait spin = default;
            while (true)
            {
                int cur = Volatile.Read(ref state.Count);

                if (cur >= state.MaxCount)
                {
                    // Log at Debug rather than Warning: quota rejection is normal business behaviour.
                    // The caller (DirectoryQuotaManager) will throw DirectoryQuotaExceededException,
                    // which is the correct place to surface the rejection at a higher log level.
                    _logger.LogDebug(
                        "Cannot increment directory {DirectoryPath} for tenant {TenantId}: limit {MaxCount} reached",
                        directoryPath, tenantId, state.MaxCount);
                    return Task.FromResult(false);
                }

                if (Interlocked.CompareExchange(ref state.Count, cur + 1, cur) == cur)
                    break;

                spin.SpinOnce();
            }

            MarkDirty(tenantId, directoryPath, state);

            _logger.LogDebug(
                "Incremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                directoryPath, tenantId, Volatile.Read(ref state.Count), state.MaxCount);

            return Task.FromResult(true);
        }

        /// <summary>
        /// Atomically decrements the file count for a directory.
        /// Lock-free hot path: uses CAS on the atomic counter.
        /// SQLite is updated asynchronously by the Write-Behind timer.
        /// </summary>
        public Task DecrementAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var state = GetOrCreateAtomicState(tenantId, directoryPath);

            // CAS decrement: don't go below 0.
            SpinWait spin = default;
            while (true)
            {
                int cur = Volatile.Read(ref state.Count);

                if (cur <= 0)
                {
                    _logger.LogWarning(
                        "Attempted to decrement count for directory {DirectoryPath}, Tenant: {TenantId} but count is already 0",
                        directoryPath, tenantId);
                    return Task.CompletedTask;
                }

                if (Interlocked.CompareExchange(ref state.Count, cur - 1, cur) == cur)
                    break;

                spin.SpinOnce();
            }

            MarkDirty(tenantId, directoryPath, state);

            _logger.LogDebug(
                "Decremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                directoryPath, tenantId, Volatile.Read(ref state.Count), state.MaxCount);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Unconditionally increments the file count for a directory, bypassing limit checks.
        /// Used only for compensation (rollback of a prior failed decrement) to restore
        /// an accurate counter without risk of spurious quota rejection.
        /// Lock-free hot path: uses Interlocked.Increment on the atomic counter.
        /// SQLite is updated asynchronously by the Write-Behind timer.
        /// </summary>
        public Task ForceIncrementAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var state = GetOrCreateAtomicState(tenantId, directoryPath);

            Interlocked.Increment(ref state.Count);
            MarkDirty(tenantId, directoryPath, state);

            _logger.LogDebug(
                "Force-incremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                directoryPath, tenantId, Volatile.Read(ref state.Count), state.MaxCount);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Gets all directory quotas for a tenant.
        /// Each entry's CurrentCount reflects the live atomic value.
        /// No lock needed: GetDatabase uses LazyThreadSafetyMode.ExecutionAndPublication
        /// and _quotaCache is a ConcurrentDictionary (lock-free reads).
        /// </summary>
        public Task<IEnumerable<DirectoryQuota>> GetAllAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var cache = GetCache(tenantId);
            var result = new List<DirectoryQuota>(cache.Count);

            foreach (var quota in cache.Values)
            {
                result.Add(MergeWithLiveCount(tenantId, quota));
            }

            return Task.FromResult<IEnumerable<DirectoryQuota>>(result);
        }

        /// <summary>
        /// Removes a directory quota.
        /// </summary>
        public async Task<bool> RemoveAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                // Remove from memory cache
                var cache = GetCache(tenantId);
                var removed = cache.TryRemove(directoryPath, out _);

                if (removed)
                {
                    // Also remove atomic state so a future GetOrCreateAtomicState reinitializes cleanly.
                    if (_atomicCounters.TryGetValue(tenantId, out var tenantCounters))
                        tenantCounters.TryRemove(directoryPath, out _);

                    if (_dirtyDirectoryKeysByTenant.TryGetValue(tenantId, out var dirtyDirectories))
                        dirtyDirectories.TryRemove(directoryPath, out _);

                    // Remove from SQLite
                    lock (GetDatabaseLock(tenantId))
                    {
                        var conn = GetDatabase(tenantId);
                        DeleteQuota(conn, directoryPath);
                    }

                    _logger.LogDebug("Removed quota for directory: {DirectoryPath}, Tenant: {TenantId}", directoryPath, tenantId);
                }

                return removed;
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Optimizes a specific tenant's quota database via SQLite VACUUM to reclaim space.
        /// </summary>
        public Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();
            var dbPath = _fileSystem.Path.Combine(_quotaDirectory, tenantId, "quotas.db");

            long sizeBefore = 0;
            long sizeAfter = 0;
            try
            {
                if (!_fileSystem.File.Exists(dbPath))
                    return Task.FromResult((0L, 0L));

                sizeBefore = _fileSystem.FileInfo.New(dbPath).Length;

                // VACUUM requires exclusive write access to the database file.
                // Running it on the shared long-lived connection (Cache=Shared) can fail
                // when concurrent readers hold shared-cache locks on the same file.
                // Strategy: close and pool-clear the tenant connection, run VACUUM on a
                // dedicated private connection (no Cache=Shared), then let the next
                // GetDatabase call transparently re-open the shared connection.
                lock (GetDatabaseLock(tenantId))
                {
                    // Flush dirty counters first so no in-memory changes are lost.
                    FlushDirtyCountersForTenant(tenantId);

                    // Step 1: evict the cached connection so GetDatabase rebuilds it afterwards.
                    _databases.TryRemove(tenantId, out var lazyConn);
                    if (lazyConn != null && lazyConn.IsValueCreated)
                    {
                        try { lazyConn.Value?.Dispose(); }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(ex, "Error closing quota connection before VACUUM for tenant {TenantId}", tenantId);
                        }
                    }

                    // Step 2: release all pooled handles so the file is exclusively ours.
                    SqliteConnection.ClearAllPools();

                    // Step 3: run VACUUM on a dedicated private connection that does NOT use
                    // Cache=Shared, ensuring SQLite can obtain the required exclusive lock.
                    var vacuumConnStr = $"Data Source={dbPath};Mode=ReadWriteCreate";
                    using (var vacuumConn = new SqliteConnection(vacuumConnStr))
                    {
                        vacuumConn.Open();
                        using (var cmd = vacuumConn.CreateCommand())
                        {
                            cmd.CommandText = "VACUUM;";
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // Step 4: release the private connection handle so the shared connection
                    // can re-open cleanly on next access.
                    SqliteConnection.ClearAllPools();
                }

                if (_fileSystem.File.Exists(dbPath))
                    sizeAfter = _fileSystem.FileInfo.New(dbPath).Length;

                _logger.LogDebug("SQLite VACUUM completed for quota tenant {TenantId}: {Before} -> {After} bytes", tenantId, sizeBefore, sizeAfter);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to optimize SQLite quota database for tenant {TenantId}", tenantId);
            }

            return Task.FromResult((sizeBefore, sizeAfter));
        }

        /// <summary>
        /// Gets all tenant IDs that have quota databases.
        /// </summary>
        public Task<IEnumerable<string>> GetAllTenantIdsAsync(CancellationToken ct)
        {
            if (!_fileSystem.Directory.Exists(_quotaDirectory))
                return Task.FromResult<IEnumerable<string>>(Array.Empty<string>());

            // Each tenant quota database lives in its own subdirectory: {quotaDirectory}/{tenantId}/quotas.db
            // Enumerate subdirectories to discover tenant IDs without any filename filtering.
            var tenantDirs = _fileSystem.Directory.GetDirectories(_quotaDirectory);
            var tenantIds = tenantDirs
                .Select(d => _fileSystem.Path.GetFileName(d))
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .ToList();

            return Task.FromResult<IEnumerable<string>>(tenantIds);
        }

        /// <summary>
        /// A scoped handle returned by <see cref="BeginDatabaseRebuildAsync"/> that holds
        /// the exclusive tenant lock for the duration of a quota database rebuild operation.
        /// Disposing this handle releases the lock and unblocks all pending operations
        /// for the tenant. Use with a <c>using</c> declaration to guarantee release.
        /// </summary>
        public sealed class DatabaseRebuildLockHandle : IDisposable
        {
            private readonly DirectoryQuotaRepository _owner;
            private readonly string _tenantId;
            private int _disposed;

            /// <summary>Path to the backup of the corrupted database, or null if no database existed.</summary>
            public string? BackupPath { get; }

            internal DatabaseRebuildLockHandle(DirectoryQuotaRepository owner, string tenantId, string? backupPath)
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
        public async Task<DatabaseRebuildLockHandle> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // Acquire exclusive lock - this will block all operations for this tenant.
            // The lock is released when the caller disposes the returned handle.
            var tenantLock = GetTenantLock(tenantId);
            await tenantLock.WaitAsync(ct);

            try
            {
                AddTenantLockBypass(tenantId);
                _logger.LogWarning("Beginning quota database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);
                var backupPath = RebuildDatabaseNoLock(tenantId);
                return new DatabaseRebuildLockHandle(this, tenantId, backupPath);
            }
            catch
            {
                // If anything fails, release the lock immediately before propagating the exception.
                RemoveTenantLockBypass(tenantId);
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
                RemoveTenantLockBypass(tenantId);
                tenantLock.Release();
                _logger.LogInformation("Quota database rebuild completed for tenant {TenantId}. Operations unblocked.", tenantId);
            }
        }

        /// <summary>
        /// Disposes the repository: stops the flush timer, performs a final SQLite flush of all
        /// dirty counters, and closes all database connections.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Stop the timer and wait briefly for an in-flight callback to complete.
            // Without this, a callback may race with Dispose and throw ObjectDisposedException
            // from _flushTimer.Change(...), which can terminate the test host process.
            if (_flushTimer != null)
            {
                using (var timerDisposed = new ManualResetEvent(false))
                {
                    try
                    {
                        _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
                        _flushTimer.Dispose(timerDisposed);
                        timerDisposed.WaitOne(TimeSpan.FromSeconds(2));
                    }
                    catch (ObjectDisposedException)
                    {
                        // Already disposed by a concurrent shutdown path.
                    }
                }
            }

            // Final flush: persist any remaining dirty counters before closing databases.
            try
            {
                DoFlushAllDirtyCounters();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during final quota counter flush on dispose");
            }

            // Close all connections
            foreach (var lazyConn in _databases.Values)
            {
                try
                {
                    if (lazyConn.IsValueCreated)
                        lazyConn.Value?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing SQLite quota connection");
                }
            }

            _databases.Clear();
            _databaseLocks.Clear();
            _quotaCache.Clear();
            _atomicCounters.Clear();
            _dirtyDirectoryKeysByTenant.Clear();
            _dirtyTenants.Clear();

            // Dispose all tenant locks
            foreach (var lockSem in _tenantLocks.Values)
            {
                try
                {
                    lockSem?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error disposing tenant lock");
                }
            }
            _tenantLocks.Clear();

            _logger.LogInformation("DirectoryQuotaRepository disposed");
        }
    }
}
