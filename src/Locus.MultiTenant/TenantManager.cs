using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Exceptions;
using Locus.Core.Models;
using Locus.MultiTenant.Models;
using Microsoft.Extensions.Logging;

namespace Locus.MultiTenant
{
    /// <summary>
    /// Manages tenant lifecycle and status using file-based storage with in-memory caching.
    /// </summary>
    public class TenantManager : ITenantManager
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<TenantManager> _logger;
        private readonly string _metadataRoot;
        private readonly TimeSpan _cacheExpiration;
        private readonly bool _autoCreateTenants;

        // Cache: TenantId → (TenantContext, ExpirationTime)
        private readonly ConcurrentDictionary<string, (ITenantContext Context, DateTime ExpiresAt)> _cache;

        // Locks for file operations per tenant
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;
        private readonly SemaphoreSlim _allTenantsCacheLock;
        private volatile AllTenantsCacheEntry? _allTenantsCache;
        private static readonly TimeSpan DefaultAllTenantsCacheTtl = TimeSpan.FromSeconds(60);

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantManager"/> class.
        /// </summary>
        /// <param name="fileSystem">The file system abstraction.</param>
        /// <param name="logger">The logger.</param>
        /// <param name="metadataRoot">The root directory for tenant metadata files. Defaults to ".locus/tenants".</param>
        /// <param name="cacheExpiration">The cache expiration time. Defaults to 5 minutes.</param>
        /// <param name="autoCreateTenants">Whether to automatically create tenants when they don't exist. Defaults to false.</param>
        public TenantManager(
            IFileSystem fileSystem,
            ILogger<TenantManager> logger,
            string? metadataRoot = null,
            TimeSpan? cacheExpiration = null,
            bool autoCreateTenants = false)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _metadataRoot = metadataRoot ?? Path.Combine(".locus", "tenants");
            _cacheExpiration = cacheExpiration ?? TimeSpan.FromMinutes(5);
            _autoCreateTenants = autoCreateTenants;

            _cache = new ConcurrentDictionary<string, (ITenantContext, DateTime)>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _allTenantsCacheLock = new SemaphoreSlim(1, 1);

            // Ensure metadata directory exists
            if (!_fileSystem.Directory.Exists(_metadataRoot))
            {
                _fileSystem.Directory.CreateDirectory(_metadataRoot);
            }
        }

        /// <inheritdoc/>
        public async Task<ITenantContext> GetTenantAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be null or whitespace.", nameof(tenantId));

            // Fast path: cache hit (no lock required).
            // No debug log here — this is the hot path called on every file operation
            // and logging on each cache hit generates excessive noise at high throughput.
            if (_cache.TryGetValue(tenantId, out var cached) && DateTime.UtcNow < cached.ExpiresAt)
                return cached.Context;

            // Slow path: cache miss or expiry.
            // Acquire per-tenant lock so only one thread reads the file — all other concurrent
            // callers for the same tenantId wait here and then get the freshly cached value.
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct);
            try
            {
                // Double-check: another thread may have populated the cache while we waited.
                if (_cache.TryGetValue(tenantId, out cached) && DateTime.UtcNow < cached.ExpiresAt)
                    return cached.Context;

                // Remove any stale entry before loading from disk.
                _cache.TryRemove(tenantId, out _);

                // Load from file (only one thread does this per cache-expiry cycle).
                var metadata = await LoadTenantMetadataAsync(tenantId, ct);
                if (metadata == null)
                {
                    if (_autoCreateTenants)
                    {
                        _logger.LogInformation("Auto-creating tenant: {TenantId}", tenantId);
                        await CreateTenantInternalAsync(tenantId, ct);

                        metadata = await LoadTenantMetadataAsync(tenantId, ct);
                        if (metadata == null)
                            throw new InvalidOperationException($"Failed to create tenant '{tenantId}'");
                    }
                    else
                    {
                        throw new TenantNotFoundException(tenantId);
                    }
                }

                var context = new TenantContext(metadata.TenantId, metadata.Status);
                _cache[tenantId] = (context, DateTime.UtcNow.Add(_cacheExpiration));

                _logger.LogDebug("Tenant {TenantId} loaded from file system", tenantId);
                return context;
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<bool> IsTenantEnabledAsync(string tenantId, CancellationToken ct)
        {
            try
            {
                var tenant = await GetTenantAsync(tenantId, ct);
                return tenant.Status == TenantStatus.Enabled;
            }
            catch (TenantNotFoundException)
            {
                return false;
            }
        }

        /// <inheritdoc/>
        public async Task EnableTenantAsync(string tenantId, CancellationToken ct)
        {
            await UpdateTenantStatusAsync(tenantId, TenantStatus.Enabled, ct);
            _logger.LogInformation("Tenant {TenantId} enabled", tenantId);
        }

        /// <inheritdoc/>
        public async Task DisableTenantAsync(string tenantId, CancellationToken ct)
        {
            await UpdateTenantStatusAsync(tenantId, TenantStatus.Disabled, ct);
            _logger.LogInformation("Tenant {TenantId} disabled", tenantId);
        }

        /// <inheritdoc/>
        public async Task CreateTenantAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be null or whitespace.", nameof(tenantId));

            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct);
            try
            {
                await CreateTenantInternalAsync(tenantId, ct);
            }
            finally
            {
                tenantLock.Release();
            }
        }

        /// <summary>
        /// Core tenant creation logic. Caller MUST hold the per-tenant SemaphoreSlim lock.
        /// </summary>
        private async Task CreateTenantInternalAsync(string tenantId, CancellationToken ct)
        {
            var metadataPath = GetTenantMetadataPath(tenantId);

            if (_fileSystem.File.Exists(metadataPath))
                throw new InvalidOperationException($"Tenant '{tenantId}' already exists.");

            var metadata = new TenantMetadata
            {
                TenantId = tenantId,
                Status = TenantStatus.Enabled,
                CreatedAt = DateTime.UtcNow,
                UpdatedAt = DateTime.UtcNow,
                StoragePath = Path.Combine("storage", tenantId)
            };

            await SaveTenantMetadataAsync(metadata, ct);

            var storagePath = metadata.StoragePath;
            if (!_fileSystem.Directory.Exists(storagePath))
                _fileSystem.Directory.CreateDirectory(storagePath);

            _cache.TryRemove(tenantId, out _);
            InvalidateAllTenantsCache();

            _logger.LogInformation("Tenant {TenantId} created successfully", tenantId);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<ITenantContext>> GetAllTenantsAsync(CancellationToken ct)
        {
            var cachedSnapshot = _allTenantsCache;
            if (cachedSnapshot != null && DateTime.UtcNow < cachedSnapshot.ExpiresAtUtc)
                return cachedSnapshot.Tenants;

            await _allTenantsCacheLock.WaitAsync(ct);
            try
            {
                cachedSnapshot = _allTenantsCache;
                if (cachedSnapshot != null && DateTime.UtcNow < cachedSnapshot.ExpiresAtUtc)
                    return cachedSnapshot.Tenants;

                var metadataFiles = _fileSystem.Directory.GetFiles(_metadataRoot, "*.json");
                var tenants = new List<ITenantContext>(metadataFiles.Length);

                foreach (var filePath in metadataFiles)
                {
                    ct.ThrowIfCancellationRequested();
                    try
                    {
                        // Read the JSON file directly — bypasses GetTenantAsync's per-tenant lock
                        // and avoids N sequential lock acquisitions for large tenant counts.
                        TenantMetadata? metadata;
                        using (var stream = _fileSystem.File.OpenRead(filePath))
                        {
                            metadata = await JsonSerializer.DeserializeAsync<TenantMetadata>(stream, JsonOptions, ct);
                        }

                        if (metadata == null)
                            continue;

                        var context = new TenantContext(metadata.TenantId, metadata.Status);
                        tenants.Add(context);

                        // Populate cache so subsequent GetTenantAsync calls are served from memory.
                        _cache[metadata.TenantId] = (context, DateTime.UtcNow.Add(_cacheExpiration));
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to load tenant metadata from {FilePath}", filePath);
                    }
                }

                var snapshot = tenants.ToArray();
                var ttl = _cacheExpiration <= TimeSpan.Zero
                    ? DefaultAllTenantsCacheTtl
                    : (_cacheExpiration < DefaultAllTenantsCacheTtl ? _cacheExpiration : DefaultAllTenantsCacheTtl);
                _allTenantsCache = new AllTenantsCacheEntry(snapshot, DateTime.UtcNow.Add(ttl));
                return snapshot;
            }
            finally
            {
                _allTenantsCacheLock.Release();
            }
        }

        private async Task UpdateTenantStatusAsync(string tenantId, TenantStatus newStatus, CancellationToken ct)
        {
            var tenantLock = _tenantLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await tenantLock.WaitAsync(ct);

            try
            {
                var metadata = await LoadTenantMetadataAsync(tenantId, ct);
                if (metadata == null)
                {
                    throw new TenantNotFoundException(tenantId);
                }

                metadata.Status = newStatus;
                metadata.UpdatedAt = DateTime.UtcNow;

                await SaveTenantMetadataAsync(metadata, ct);

                // Invalidate cache
                _cache.TryRemove(tenantId, out _);
                InvalidateAllTenantsCache();
            }
            finally
            {
                tenantLock.Release();
            }
        }

        private async Task<TenantMetadata?> LoadTenantMetadataAsync(string tenantId, CancellationToken ct)
        {
            var metadataPath = GetTenantMetadataPath(tenantId);

            if (!_fileSystem.File.Exists(metadataPath))
            {
                return null;
            }

            try
            {
                using (var stream = _fileSystem.File.OpenRead(metadataPath))
                {
                    return await JsonSerializer.DeserializeAsync<TenantMetadata>(stream, JsonOptions, ct);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load tenant metadata for {TenantId}", tenantId);
                throw;
            }
        }

        private async Task SaveTenantMetadataAsync(TenantMetadata metadata, CancellationToken ct)
        {
            var metadataPath = GetTenantMetadataPath(metadata.TenantId);

            // Ensure directory exists
            var directory = _fileSystem.Path.GetDirectoryName(metadataPath);
            if (directory != null && !_fileSystem.Directory.Exists(directory))
            {
                _fileSystem.Directory.CreateDirectory(directory);
            }

            using (var stream = _fileSystem.File.Create(metadataPath))
            {
                await JsonSerializer.SerializeAsync(stream, metadata, JsonOptions, ct);
            }
        }

        private string GetTenantMetadataPath(string tenantId)
        {
            return _fileSystem.Path.Combine(_metadataRoot, $"{tenantId}.json");
        }

        private void InvalidateAllTenantsCache()
        {
            _allTenantsCache = null;
        }

        private sealed class AllTenantsCacheEntry
        {
            public AllTenantsCacheEntry(IReadOnlyList<ITenantContext> tenants, DateTime expiresAtUtc)
            {
                Tenants = tenants;
                ExpiresAtUtc = expiresAtUtc;
            }

            public IReadOnlyList<ITenantContext> Tenants { get; }

            public DateTime ExpiresAtUtc { get; }
        }
    }
}
