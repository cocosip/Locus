using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using Microsoft.Extensions.Logging;
using LiteDBOptions = Locus.Core.Models.LiteDBOptions;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Repository for directory quota storage using per-tenant LiteDB with in-memory caching.
    /// Hot path (TryIncrementAsync / DecrementAsync) is lock-free using CAS atomic counters.
    /// LiteDB writes are deferred via a Write-Behind timer (every 5 seconds) for performance.
    /// Thread-safe for concurrent access.
    /// </summary>
    public class DirectoryQuotaRepository : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DirectoryQuotaRepository> _logger;
        private readonly string _quotaDirectory;
        private readonly LiteDBOptions _liteDbOptions;

        // Per-tenant in-memory cache (source of truth for MaxCount / Enabled config)
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>> _quotaCache;

        // Per-tenant LiteDB databases (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<LiteDatabase>> _databases;

        // Per-tenant operation locks (used for config operations; NOT used in hot path)
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;
        private static readonly AsyncLocal<HashSet<string>> _tenantLockBypass = new AsyncLocal<HashSet<string>>();

        // --- Write-Behind: atomic counters + background flush timer ---

        /// <summary>
        /// Per-tenant atomic counter state for lock-free hot path.
        /// Count is the authoritative live value; LiteDB is updated asynchronously.
        /// </summary>
        private sealed class AtomicQuotaState
        {
            /// <summary>Live file count. Use Interlocked operations only.</summary>
            public int Count;

            /// <summary>Maximum allowed count (0 = unlimited). Updated by UpdateAsync.</summary>
            public volatile int MaxCount;

            /// <summary>Whether quota enforcement is active. Updated by UpdateAsync.</summary>
            public volatile bool Enabled;

            /// <summary>True when Count has changed since last LiteDB flush.</summary>
            public volatile bool Dirty;
        }

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, AtomicQuotaState>> _atomicCounters;
        private readonly Timer _flushTimer;
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
            LiteDBOptions? liteDbOptions = null)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(quotaDirectory))
                throw new ArgumentException("Quota directory cannot be empty", nameof(quotaDirectory));

            _quotaDirectory = quotaDirectory;
            _liteDbOptions = liteDbOptions ?? new LiteDBOptions();
            _quotaCache = new ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>>();
            _databases = new ConcurrentDictionary<string, Lazy<LiteDatabase>>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _atomicCounters = new ConcurrentDictionary<string, ConcurrentDictionary<string, AtomicQuotaState>>();

            // Ensure quota directory exists
            if (!_fileSystem.Directory.Exists(_quotaDirectory))
            {
                _fileSystem.Directory.CreateDirectory(_quotaDirectory);
            }

            _logger.LogInformation(
                "DirectoryQuotaRepository initialized at {Directory} with LiteDB: Journal={Journal}, Checkpoint={Checkpoint}, Timeout={Timeout}s",
                _quotaDirectory,
                _liteDbOptions.EnableJournal,
                _liteDbOptions.CheckpointInterval,
                _liteDbOptions.TimeoutSeconds);

            // Start the Write-Behind flush timer (non-reentrant: fires once then reschedules)
            _flushTimer = new Timer(FlushDirtyCounters, null, FlushIntervalMs, Timeout.Infinite);
        }

        /// <summary>
        /// Gets or creates a LiteDB database for a tenant.
        /// </summary>
        private LiteDatabase GetDatabase(string tenantId)
        {
            // Use Lazy<LiteDatabase> to ensure thread-safe initialization
            // This prevents multiple threads from simultaneously creating the database instance
            var lazyDb = _databases.GetOrAdd(tenantId, tid => new Lazy<LiteDatabase>(() =>
            {
                try
                {
                    // Ensure quota directory exists (thread-safe in case of concurrent access)
                    if (!_fileSystem.Directory.Exists(_quotaDirectory))
                    {
                        _fileSystem.Directory.CreateDirectory(_quotaDirectory);
                    }

                    var dbPath = _fileSystem.Path.Combine(_quotaDirectory, $"{tid}-quotas.db");
                    // Use configured LiteDB options (WAL mode strongly recommended for K8s/network storage)
                    var connectionString = _liteDbOptions.BuildConnectionString(dbPath);
                    var db = new LiteDatabase(connectionString);

                    var quotas = db.GetCollection<DirectoryQuota>("quotas");
                    quotas.EnsureIndex(x => x.DirectoryPath, unique: true);
                    quotas.EnsureIndex(x => x.Enabled);

                    _logger.LogDebug("Created/opened LiteDB quota database for tenant {TenantId} at {Path}", tid, dbPath);

                    // Load all quotas into memory on first access
                    LoadQuotasForTenant(tid, quotas);

                    return db;
                }
                catch (Exception ex) when (IsRecoverableDatabaseException(ex))
                {
                    _logger.LogError(ex,
                        "CORRUPTED QUOTA DATABASE DETECTED for tenant {TenantId} during initialization. Attempting automatic recovery...",
                        tid);

                    try
                    {
                        // Remove from cache to force rebuild
                        _databases.TryRemove(tid, out _);
                        _quotaCache.TryRemove(tid, out _);

                        // Rebuild database synchronously (we're in Lazy initialization)
                        var backupPath = RebuildDatabaseNoLock(tid);

                        _logger.LogWarning(
                            "Quota database rebuilt for tenant {TenantId} during initialization. Backup: {BackupPath}",
                            tid, backupPath ?? "N/A");

                        // Retry initialization with configured LiteDB options
                        var dbPath = _fileSystem.Path.Combine(_quotaDirectory, $"{tid}-quotas.db");
                        var connectionString = _liteDbOptions.BuildConnectionString(dbPath);
                        var db = new LiteDatabase(connectionString);

                        var quotas = db.GetCollection<DirectoryQuota>("quotas");
                        quotas.EnsureIndex(x => x.DirectoryPath, unique: true);
                        quotas.EnsureIndex(x => x.Enabled);

                        _logger.LogInformation(
                            "Successfully recovered and initialized quota database for tenant {TenantId}",
                            tid);

                        return db;
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

            // Access the Value property to trigger initialization if needed
            return lazyDb.Value;
        }

        /// <summary>
        /// Loads all quotas into memory for a tenant.
        /// </summary>
        private void LoadQuotasForTenant(string tenantId, ILiteCollection<DirectoryQuota> collection)
        {
            var quotas = collection.FindAll().ToList();

            if (quotas.Count > 0)
            {
                var cache = _quotaCache.GetOrAdd(tenantId, _ => new ConcurrentDictionary<string, DirectoryQuota>());
                foreach (var quota in quotas)
                {
                    cache[quota.DirectoryPath] = quota;
                }

                _logger.LogInformation("Loaded {Count} directory quotas for tenant {TenantId} into memory",
                    quotas.Count, tenantId);
            }
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
        /// Checks whether the exception indicates a recoverable database corruption scenario.
        /// </summary>
        private static bool IsRecoverableDatabaseException(Exception ex)
        {
            if (ex is ArgumentOutOfRangeException || ex is OverflowException || ex is IOException)
                return true;

            if (ex is LiteException liteEx)
            {
                var message = liteEx.Message ?? string.Empty;
                return message.Contains("ReadFull")
                    || message.Contains("PAGE_SIZE")
                    || message.Contains("Checkpoint")
                    || message.Contains("invalid")
                    || message.Contains("corrupt");
            }

            return false;
        }

        /// <summary>
        /// Rebuilds a tenant quota database in-place.
        /// Must be called while holding the tenant lock.
        /// </summary>
        private string? RebuildDatabaseNoLock(string tenantId)
        {
            var dbPath = _fileSystem.Path.Combine(_quotaDirectory, $"{tenantId}-quotas.db");

            if (!_fileSystem.File.Exists(dbPath))
            {
                _logger.LogInformation("No quota database file to rebuild for tenant {TenantId}", tenantId);
                return null;
            }

            // Dispose existing database connection
            if (_databases.TryGetValue(tenantId, out var lazyDb) && lazyDb.IsValueCreated)
            {
                try
                {
                    lazyDb.Value?.Dispose();
                    _logger.LogDebug("Disposed quota database connection for rebuild: {TenantId}", tenantId);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing quota database connection for tenant {TenantId}", tenantId);
                }
            }

            // Remove from caches before rebuilding
            _databases.TryRemove(tenantId, out _);
            _quotaCache.TryRemove(tenantId, out _);

            // Backup and delete corrupted database
            var backupPath = $"{dbPath}.corrupted.{DateTime.UtcNow:yyyyMMddHHmmss}";
            _fileSystem.File.Copy(dbPath, backupPath, overwrite: true);
            _logger.LogInformation("Backed up corrupted quota database: {BackupPath}", backupPath);

            _fileSystem.File.Delete(dbPath);
            _logger.LogInformation("Deleted corrupted quota database: {DatabasePath}", dbPath);

            return backupPath;
        }

        /// <summary>
        /// Persists a quota change with automatic recovery on corruption.
        /// Must be called while holding the tenant lock.
        /// </summary>
        private void PersistQuotaWithRecoveryNoLock(string tenantId, DirectoryQuota quota, bool useUpsert)
        {
            try
            {
                var db = GetDatabase(tenantId);
                var quotas = db.GetCollection<DirectoryQuota>("quotas");

                if (useUpsert)
                    quotas.Upsert(quota);
                else
                    quotas.Update(quota);

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

                    var db = GetDatabase(tenantId);
                    var quotas = db.GetCollection<DirectoryQuota>("quotas");
                    quotas.Upsert(quota);

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
        /// Timer callback: flushes all dirty atomic counters to LiteDB.
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
                    _flushTimer.Change(FlushIntervalMs, Timeout.Infinite);
            }
        }

        /// <summary>
        /// Core flush logic: iterates all tenants and writes dirty counter states to LiteDB.
        /// Safe to call from Dispose for a final drain.
        /// </summary>
        private void DoFlushAllDirtyCounters()
        {
            foreach (var tenantPair in _atomicCounters)
            {
                var tenantId = tenantPair.Key;
                var tenantCounters = tenantPair.Value;

                foreach (var dirPair in tenantCounters)
                {
                    var directoryPath = dirPair.Key;
                    var atomicState = dirPair.Value;

                    if (!atomicState.Dirty)
                        continue;

                    try
                    {
                        // Clear dirty BEFORE reading count so any concurrent increments that
                        // happen during the LiteDB write will mark Dirty=true again and be
                        // captured in the next flush cycle.
                        atomicState.Dirty = false;
                        var count = Volatile.Read(ref atomicState.Count);

                        // Get or create the cache entry; if it doesn't exist yet (first-ever
                        // increment for this directory before GetOrCreateAsync was called),
                        // create a minimal entry so we can persist it.
                        var cache = _quotaCache.GetOrAdd(
                            tenantId,
                            _ => new ConcurrentDictionary<string, DirectoryQuota>());

                        var quota = cache.GetOrAdd(directoryPath, path => new DirectoryQuota
                        {
                            DirectoryPath = path,
                            CurrentCount = count,
                            MaxCount = atomicState.MaxCount,
                            Enabled = atomicState.Enabled,
                            CreatedAt = DateTime.UtcNow,
                            LastUpdated = DateTime.UtcNow
                        });

                        // Update the cached entry in-place with the live count.
                        quota.CurrentCount = count;
                        quota.LastUpdated = DateTime.UtcNow;

                        // Persist to LiteDB.
                        var db = GetDatabase(tenantId);
                        var quotas = db.GetCollection<DirectoryQuota>("quotas");
                        quotas.Upsert(quota);

                        _logger.LogDebug(
                            "Flushed quota counter for {TenantId}/{DirectoryPath}: count={Count}",
                            tenantId, directoryPath, count);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,
                            "Failed to flush quota counter for {TenantId}/{DirectoryPath}; will retry on next flush",
                            tenantId, directoryPath);

                        // Re-mark dirty so the next flush retries.
                        atomicState.Dirty = true;
                    }
                }
            }
        }

        // -----------------------------------------------------------------
        // Public API
        // -----------------------------------------------------------------

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
        /// Persists to LiteDB immediately and syncs the atomic state so the hot path
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
        /// Atomically increments the file count for a directory.
        /// Lock-free hot path: uses CAS (compare-and-swap) on the atomic counter.
        /// LiteDB is updated asynchronously by the Write-Behind timer.
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
                state.Dirty = true;
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

            state.Dirty = true;

            _logger.LogDebug(
                "Incremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                directoryPath, tenantId, Volatile.Read(ref state.Count), state.MaxCount);

            return Task.FromResult(true);
        }

        /// <summary>
        /// Atomically decrements the file count for a directory.
        /// Lock-free hot path: uses CAS on the atomic counter.
        /// LiteDB is updated asynchronously by the Write-Behind timer.
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

            state.Dirty = true;

            _logger.LogDebug(
                "Decremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
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

                    // Remove from LiteDB
                    var db = GetDatabase(tenantId);
                    var quotas = db.GetCollection<DirectoryQuota>("quotas");
                    quotas.Delete(directoryPath);

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
        /// Optimizes (rebuilds) a specific tenant's quota database to reclaim space.
        /// This method is thread-safe and will block all operations for this tenant during optimization.
        /// WARNING: This is a heavy operation. Should be called during maintenance windows.
        /// </summary>
        public Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct)
        {
            var dbPath = _fileSystem.Path.Combine(_quotaDirectory, $"{tenantId}-quotas.db");

            return LiteDbOptimizationHelper.OptimizeDatabaseAsync(
                tenantId,
                dbPath,
                _databases,
                _tenantLocks,
                _fileSystem,
                _logger,
                "quota",
                ct);
        }

        /// <summary>
        /// Gets all tenant IDs that have quota databases.
        /// </summary>
        public Task<IEnumerable<string>> GetAllTenantIdsAsync(CancellationToken ct)
        {
            if (!_fileSystem.Directory.Exists(_quotaDirectory))
                return Task.FromResult<IEnumerable<string>>(Array.Empty<string>());

            var dbFiles = _fileSystem.Directory.GetFiles(_quotaDirectory, "*-quotas.db");
            var tenantIds = dbFiles
                .Select(f =>
                {
                    var fileName = _fileSystem.Path.GetFileNameWithoutExtension(f);
                    // Remove "-quotas" suffix
                    return fileName.EndsWith("-quotas") ? fileName.Substring(0, fileName.Length - 7) : fileName;
                })
                .Where(name => !string.IsNullOrWhiteSpace(name)
                    && !name.Contains("-backup", StringComparison.OrdinalIgnoreCase)    // Filter LiteDB backup files: "tenant-001-quotas.db-backup-1"
                    && !name.Contains(".corrupted.", StringComparison.OrdinalIgnoreCase) // Filter corruption backups
                    && !name.EndsWith("-journal", StringComparison.OrdinalIgnoreCase))   // Filter LiteDB journal files
                .ToList();

            return Task.FromResult<IEnumerable<string>>(tenantIds);
        }

        /// <summary>
        /// Prepares for database rebuild by safely disposing connections and backing up the database.
        /// Thread-safe: Acquires exclusive lock for this tenant, blocking all operations.
        /// IMPORTANT: Caller must call FinishDatabaseRebuild() to release the lock.
        /// </summary>
        public async Task<string?> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // Acquire exclusive lock - this will block all operations for this tenant
            var tenantLock = GetTenantLock(tenantId);
            await tenantLock.WaitAsync(ct);

            try
            {
                AddTenantLockBypass(tenantId);
                _logger.LogWarning("Beginning quota database rebuild for tenant {TenantId}. All operations will be BLOCKED.", tenantId);
                return RebuildDatabaseNoLock(tenantId);
            }
            catch
            {
                // If anything fails, release the lock immediately
                RemoveTenantLockBypass(tenantId);
                tenantLock.Release();
                throw;
            }

            // NOTE: Lock is NOT released here - it will be released in FinishDatabaseRebuild()
        }

        /// <summary>
        /// Finishes database rebuild by releasing the tenant lock.
        /// IMPORTANT: Must be called after BeginDatabaseRebuildAsync() to unblock operations.
        /// </summary>
        public void FinishDatabaseRebuild(string tenantId)
        {
            if (_tenantLocks.TryGetValue(tenantId, out var tenantLock))
            {
                RemoveTenantLockBypass(tenantId);
                tenantLock.Release();
                _logger.LogInformation("Quota database rebuild completed for tenant {TenantId}. Operations unblocked.", tenantId);
            }
        }

        /// <summary>
        /// Disposes the repository: stops the flush timer, performs a final LiteDB flush of all
        /// dirty counters, and closes all database connections.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Stop the timer to prevent new callbacks from being scheduled.
            _flushTimer.Dispose();

            // Final flush: persist any remaining dirty counters before closing databases.
            try
            {
                DoFlushAllDirtyCounters();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during final quota counter flush on dispose");
            }

            // Close all databases
            foreach (var lazyDb in _databases.Values)
            {
                try
                {
                    if (lazyDb.IsValueCreated)
                        lazyDb.Value?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing LiteDB quota database");
                }
            }

            _databases.Clear();
            _quotaCache.Clear();
            _atomicCounters.Clear();

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
