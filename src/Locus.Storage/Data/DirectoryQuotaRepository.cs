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
    /// Thread-safe for concurrent access.
    /// </summary>
    public class DirectoryQuotaRepository : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<DirectoryQuotaRepository> _logger;
        private readonly string _quotaDirectory;
        private readonly LiteDBOptions _liteDbOptions;

        // Per-tenant in-memory cache
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>> _quotaCache;

        // Per-tenant LiteDB databases (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<LiteDatabase>> _databases;

        // Per-tenant operation locks (shared by normal operations, optimization, and rebuild)
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;
        private static readonly AsyncLocal<HashSet<string>> _tenantLockBypass = new AsyncLocal<HashSet<string>>();

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
        /// Gets or creates a tenant-specific lock shared by all quota operations.
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

        /// <summary>
        /// Gets or creates a directory quota.
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
                return GetOrCreateNoLock(tenantId, directoryPath);
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Gets a directory quota by path.
        /// </summary>
        public async Task<DirectoryQuota?> GetAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                var cache = GetCache(tenantId);
                cache.TryGetValue(directoryPath, out var quota);
                return quota;
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Updates a directory quota.
        /// Thread-safe: Persists to LiteDB first, then updates memory cache.
        /// Auto-recovery: Rebuilds corrupted database and retries on LiteDB errors.
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

                _logger.LogDebug("Updated quota for directory: {DirectoryPath}, Tenant: {TenantId}, Current: {Current}, Max: {Max}",
                    quota.DirectoryPath, tenantId, quota.CurrentCount, quota.MaxCount);
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Atomically increments the file count for a directory.
        /// Returns false if the increment would exceed the maximum count.
        /// </summary>
        public async Task<bool> TryIncrementAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                var quota = GetOrCreateNoLock(tenantId, directoryPath);

                // If quota is disabled or no limit set, allow increment
                if (!quota.Enabled || quota.MaxCount == 0)
                {
                    quota.CurrentCount++;
                    quota.LastUpdated = DateTime.UtcNow;

                    PersistQuotaWithRecoveryNoLock(tenantId, quota, useUpsert: false);
                    return true;
                }

                // Check if incrementing would exceed limit
                if (quota.CurrentCount >= quota.MaxCount)
                {
                    _logger.LogWarning("Cannot increment directory {DirectoryPath} for tenant {TenantId}: limit {MaxCount} reached",
                        directoryPath, tenantId, quota.MaxCount);
                    return false;
                }

                // Increment count
                quota.CurrentCount++;
                quota.LastUpdated = DateTime.UtcNow;

                PersistQuotaWithRecoveryNoLock(tenantId, quota, useUpsert: false);

                _logger.LogDebug("Incremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                    directoryPath, tenantId, quota.CurrentCount, quota.MaxCount);

                return true;
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Atomically decrements the file count for a directory.
        /// </summary>
        public async Task DecrementAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                var quota = GetOrCreateNoLock(tenantId, directoryPath);

                // Decrement count (don't go below 0)
                if (quota.CurrentCount > 0)
                {
                    quota.CurrentCount--;
                    quota.LastUpdated = DateTime.UtcNow;

                    PersistQuotaWithRecoveryNoLock(tenantId, quota, useUpsert: false);

                    _logger.LogDebug("Decremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                        directoryPath, tenantId, quota.CurrentCount, quota.MaxCount);
                }
                else
                {
                    _logger.LogWarning("Attempted to decrement count for directory {DirectoryPath}, Tenant: {TenantId} but count is already 0",
                        directoryPath, tenantId);
                }
            }
            finally
            {
                tenantLock?.Release();
            }
        }

        /// <summary>
        /// Gets all directory quotas for a tenant.
        /// </summary>
        public async Task<IEnumerable<DirectoryQuota>> GetAllAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var tenantLock = await AcquireTenantLockIfNeededAsync(tenantId, ct);
            try
            {
                var cache = GetCache(tenantId);
                return cache.Values.ToList();
            }
            finally
            {
                tenantLock?.Release();
            }
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
        /// <param name="tenantId">The tenant ID whose database should be optimized.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Tuple of (size before, size after) in bytes.</returns>
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
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Path to the backup file, or null if no database exists.</returns>
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
        /// <param name="tenantId">The tenant ID.</param>
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
        /// Disposes the repository and closes all databases.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Close all databases
            foreach (var lazyDb in _databases.Values)
            {
                try
                {
                    // Only dispose if the Lazy value was actually created
                    if (lazyDb.IsValueCreated)
                    {
                        lazyDb.Value?.Dispose();
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing LiteDB quota database");
                }
            }

            _databases.Clear();
            _quotaCache.Clear();

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
