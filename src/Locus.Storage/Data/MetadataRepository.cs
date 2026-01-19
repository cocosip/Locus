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

namespace Locus.Storage.Data
{
    /// <summary>
    /// Repository for file metadata storage using per-tenant LiteDB with active-data in-memory caching.
    /// Only keeps Pending/Processing/Failed files in memory. Completed files are immediately deleted.
    /// Thread-safe for concurrent access.
    /// </summary>
    public class MetadataRepository : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<MetadataRepository> _logger;
        private readonly string _metadataDirectory;

        // Per-tenant in-memory cache (only active files: Pending/Processing/Failed)
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>> _activeFiles;

        // Per-tenant LiteDB databases
        private readonly ConcurrentDictionary<string, LiteDatabase> _databases;

        // Per-tenant locks for concurrent file allocation
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _tenantLocks;

        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepository"/> class.
        /// </summary>
        public MetadataRepository(
            IFileSystem fileSystem,
            ILogger<MetadataRepository> logger,
            string metadataDirectory)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            if (string.IsNullOrWhiteSpace(metadataDirectory))
                throw new ArgumentException("Metadata directory cannot be empty", nameof(metadataDirectory));

            _metadataDirectory = metadataDirectory;
            _activeFiles = new ConcurrentDictionary<string, ConcurrentDictionary<string, FileMetadata>>();
            _databases = new ConcurrentDictionary<string, LiteDatabase>();
            _tenantLocks = new ConcurrentDictionary<string, SemaphoreSlim>();

            // Ensure metadata directory exists
            if (!_fileSystem.Directory.Exists(_metadataDirectory))
            {
                _fileSystem.Directory.CreateDirectory(_metadataDirectory);
            }

            _logger.LogInformation("MetadataRepository initialized with per-tenant LiteDB storage at {Directory}", _metadataDirectory);
        }

        /// <summary>
        /// Gets or creates a LiteDB database for a tenant.
        /// </summary>
        private LiteDatabase GetDatabase(string tenantId)
        {
            return _databases.GetOrAdd(tenantId, tid =>
            {
                // Ensure metadata directory exists (thread-safe in case of concurrent access)
                if (!_fileSystem.Directory.Exists(_metadataDirectory))
                {
                    _fileSystem.Directory.CreateDirectory(_metadataDirectory);
                }

                var dbPath = _fileSystem.Path.Combine(_metadataDirectory, $"{tid}.db");
                var db = new LiteDatabase(dbPath);

                var files = db.GetCollection<FileMetadata>("files");
                files.EnsureIndex(x => x.FileKey, unique: true);
                files.EnsureIndex(x => x.Status);
                files.EnsureIndex(x => x.CreatedAt);
                files.EnsureIndex(x => x.AvailableForProcessingAt);

                _logger.LogDebug("Created/opened LiteDB database for tenant {TenantId} at {Path}", tid, dbPath);

                // Load active files into memory on first access
                LoadActiveFilesForTenant(tid, files);

                return db;
            });
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
        /// Thread-safe: Persists to LiteDB first, then updates memory cache.
        /// This ensures consistency - if persistence fails, memory won't have stale data.
        /// </summary>
        public Task AddOrUpdateAsync(FileMetadata metadata, CancellationToken ct)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.FileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(metadata));

            if (string.IsNullOrWhiteSpace(metadata.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(metadata));

            // 1. Persist to LiteDB FIRST (persistence is critical)
            // If this fails, exception is thrown and memory won't be corrupted
            var db = GetDatabase(metadata.TenantId);
            var files = db.GetCollection<FileMetadata>("files");
            files.Upsert(metadata);

            // 2. Update memory cache AFTER successful persistence
            // This operation is local and virtually guaranteed to succeed
            var cache = GetCache(metadata.TenantId);
            cache[metadata.FileKey] = metadata;

            _logger.LogDebug("Added/updated metadata for file: {FileKey}, Tenant: {TenantId}, Status: {Status}",
                metadata.FileKey, metadata.TenantId, metadata.Status);

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
                // Remove from LiteDB
                var db = GetDatabase(tenantId);
                var files = db.GetCollection<FileMetadata>("files");
                files.Delete(fileKey);

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
                    // Save original state for rollback
                    var originalStatus = file.Status;
                    var originalProcessingStartTime = file.ProcessingStartTime;

                    try
                    {
                        // Update both memory and persistence atomically
                        file.Status = FileProcessingStatus.Processing;
                        file.ProcessingStartTime = DateTime.UtcNow;

                        // Persist to LiteDB
                        var db = GetDatabase(tenantId);
                        var files = db.GetCollection<FileMetadata>("files");
                        files.Update(file);

                        _logger.LogDebug("Allocated file for processing: {FileKey}, Tenant: {TenantId}", file.FileKey, tenantId);
                    }
                    catch
                    {
                        // Rollback memory state on persistence failure
                        file.Status = originalStatus;
                        file.ProcessingStartTime = originalProcessingStartTime;
                        throw;
                    }
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

            var db = GetDatabase(tenantId);
            var files = db.GetCollection<FileMetadata>("files");

            // Save original states for rollback
            var originalStates = new Dictionary<string, (FileProcessingStatus Status, DateTime? ProcessingStartTime)>();

            try
            {
                // Atomically mark all files as Processing
                foreach (var file in candidates)
                {
                    originalStates[file.FileKey] = (file.Status, file.ProcessingStartTime);

                    file.Status = FileProcessingStatus.Processing;
                    file.ProcessingStartTime = DateTime.UtcNow;
                    files.Update(file);
                    results.Add(file);
                }

                _logger.LogDebug("Allocated {Count} files for processing, Tenant: {TenantId}", results.Count, tenantId);
            }
            catch
            {
                // Rollback all memory states on any failure
                foreach (var kvp in originalStates)
                {
                    var file = cache.Values.FirstOrDefault(f => f.FileKey == kvp.Key);
                    if (file != null)
                    {
                        file.Status = kvp.Value.Status;
                        file.ProcessingStartTime = kvp.Value.ProcessingStartTime;
                    }
                }
                throw;
            }

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
        /// Disposes the repository and closes all databases.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            // Close all databases
            foreach (var db in _databases.Values)
            {
                try
                {
                    db?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error closing LiteDB database");
                }
            }

            _databases.Clear();
            _activeFiles.Clear();

            // Dispose all tenant locks
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

            _logger.LogInformation("MetadataRepository disposed");
        }
    }
}
