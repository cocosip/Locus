using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using Microsoft.Extensions.Logging;

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

        // Per-tenant in-memory cache
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>> _quotaCache;

        // Per-tenant LiteDB databases (using Lazy for thread-safe initialization)
        private readonly ConcurrentDictionary<string, Lazy<LiteDatabase>> _databases;

        // Per-directory locks for atomic increment operations
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _directoryLocks;

        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaRepository"/> class.
        /// </summary>
        public DirectoryQuotaRepository(
            IFileSystem fileSystem,
            ILogger<DirectoryQuotaRepository> logger,
            string quotaDirectory)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(quotaDirectory))
                throw new ArgumentException("Quota directory cannot be empty", nameof(quotaDirectory));

            _quotaDirectory = quotaDirectory;
            _quotaCache = new ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuota>>();
            _databases = new ConcurrentDictionary<string, Lazy<LiteDatabase>>();
            _directoryLocks = new ConcurrentDictionary<string, SemaphoreSlim>();

            // Ensure quota directory exists
            if (!_fileSystem.Directory.Exists(_quotaDirectory))
            {
                _fileSystem.Directory.CreateDirectory(_quotaDirectory);
            }

            _logger.LogInformation("DirectoryQuotaRepository initialized with per-tenant LiteDB storage at {Directory}", _quotaDirectory);
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
                // Ensure quota directory exists (thread-safe in case of concurrent access)
                if (!_fileSystem.Directory.Exists(_quotaDirectory))
                {
                    _fileSystem.Directory.CreateDirectory(_quotaDirectory);
                }

                var dbPath = _fileSystem.Path.Combine(_quotaDirectory, $"{tid}-quotas.db");
                // Use Shared mode for concurrent access support
                var connectionString = $"Filename={dbPath};Mode=Shared";
                var db = new LiteDatabase(connectionString);

                var quotas = db.GetCollection<DirectoryQuota>("quotas");
                quotas.EnsureIndex(x => x.DirectoryPath, unique: true);
                quotas.EnsureIndex(x => x.Enabled);

                _logger.LogDebug("Created/opened LiteDB quota database for tenant {TenantId} at {Path}", tid, dbPath);

                // Load all quotas into memory on first access
                LoadQuotasForTenant(tid, quotas);

                return db;
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
        /// Gets or creates a directory quota.
        /// </summary>
        public Task<DirectoryQuota> GetOrCreateAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var cache = GetCache(tenantId);

            var quota = cache.GetOrAdd(directoryPath, path =>
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

                // Persist to LiteDB
                var db = GetDatabase(tenantId);
                var quotas = db.GetCollection<DirectoryQuota>("quotas");
                quotas.Upsert(newQuota);

                return newQuota;
            });

            return Task.FromResult(quota);
        }

        /// <summary>
        /// Gets a directory quota by path.
        /// </summary>
        public Task<DirectoryQuota?> GetAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var cache = GetCache(tenantId);
            cache.TryGetValue(directoryPath, out var quota);
            return Task.FromResult<DirectoryQuota?>(quota);
        }

        /// <summary>
        /// Updates a directory quota.
        /// Thread-safe: Persists to LiteDB first, then updates memory cache.
        /// </summary>
        public Task UpdateAsync(string tenantId, DirectoryQuota quota, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (quota == null)
                throw new ArgumentNullException(nameof(quota));

            if (string.IsNullOrWhiteSpace(quota.DirectoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(quota));

            quota.LastUpdated = DateTime.UtcNow;

            // 1. Persist to LiteDB FIRST (persistence is critical)
            // If this fails, exception is thrown and memory won't be corrupted
            var db = GetDatabase(tenantId);
            var quotas = db.GetCollection<DirectoryQuota>("quotas");
            quotas.Upsert(quota);

            // 2. Update memory cache AFTER successful persistence
            // This operation is local and virtually guaranteed to succeed
            var cache = GetCache(tenantId);
            cache[quota.DirectoryPath] = quota;

            _logger.LogDebug("Updated quota for directory: {DirectoryPath}, Tenant: {TenantId}, Current: {Current}, Max: {Max}",
                quota.DirectoryPath, tenantId, quota.CurrentCount, quota.MaxCount);

            return Task.CompletedTask;
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

            // Use per-directory lock to ensure atomic check-and-increment
            var lockKey = $"{tenantId}:{directoryPath}";
            var directoryLock = _directoryLocks.GetOrAdd(lockKey, _ => new SemaphoreSlim(1, 1));

            await directoryLock.WaitAsync(ct);
            try
            {
                return await Task.Run(() =>
                {
                    var quota = GetOrCreateAsync(tenantId, directoryPath, ct).GetAwaiter().GetResult();

                    // If quota is disabled or no limit set, allow increment
                    if (!quota.Enabled || quota.MaxCount == 0)
                    {
                        quota.CurrentCount++;
                        quota.LastUpdated = DateTime.UtcNow;

                        // Persist to LiteDB
                        var db = GetDatabase(tenantId);
                        var quotas = db.GetCollection<DirectoryQuota>("quotas");
                        quotas.Update(quota);

                        // Update cache
                        var cache = GetCache(tenantId);
                        cache[directoryPath] = quota;

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

                    // Persist to LiteDB
                    var db2 = GetDatabase(tenantId);
                    var quotas2 = db2.GetCollection<DirectoryQuota>("quotas");
                    quotas2.Update(quota);

                    // Update cache
                    var cache2 = GetCache(tenantId);
                    cache2[directoryPath] = quota;

                    _logger.LogDebug("Incremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                        directoryPath, tenantId, quota.CurrentCount, quota.MaxCount);

                    return true;
                }, ct);
            }
            finally
            {
                directoryLock.Release();
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

            var quota = await GetOrCreateAsync(tenantId, directoryPath, ct);

            // Decrement count (don't go below 0)
            if (quota.CurrentCount > 0)
            {
                quota.CurrentCount--;
                quota.LastUpdated = DateTime.UtcNow;

                // Persist to LiteDB
                var db = GetDatabase(tenantId);
                var quotas = db.GetCollection<DirectoryQuota>("quotas");
                quotas.Update(quota);

                _logger.LogDebug("Decremented count for directory {DirectoryPath}, Tenant: {TenantId}: {CurrentCount}/{MaxCount}",
                    directoryPath, tenantId, quota.CurrentCount, quota.MaxCount);
            }
            else
            {
                _logger.LogWarning("Attempted to decrement count for directory {DirectoryPath}, Tenant: {TenantId} but count is already 0",
                    directoryPath, tenantId);
            }
        }

        /// <summary>
        /// Gets all directory quotas for a tenant.
        /// </summary>
        public Task<IEnumerable<DirectoryQuota>> GetAllAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var cache = GetCache(tenantId);
            var quotas = cache.Values.ToList();
            return Task.FromResult<IEnumerable<DirectoryQuota>>(quotas);
        }

        /// <summary>
        /// Removes a directory quota.
        /// </summary>
        public Task<bool> RemoveAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

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

            return Task.FromResult(removed);
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

            // Dispose all directory locks
            foreach (var lockSem in _directoryLocks.Values)
            {
                try
                {
                    lockSem?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error disposing directory lock");
                }
            }
            _directoryLocks.Clear();

            _logger.LogInformation("DirectoryQuotaRepository disposed");
        }
    }
}
