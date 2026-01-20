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
using Locus.Core.Models;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Monitors directories for new files and automatically imports them into the storage pool.
    /// </summary>
    public class FileWatcher : IFileWatcher
    {
        private readonly IFileSystem _fileSystem;
        private readonly IStoragePool _storagePool;
        private readonly ITenantManager _tenantManager;
        private readonly ILogger<FileWatcher> _logger;
        private readonly string _configurationRoot;

        // Track imported files to avoid duplicates: filepath -> fileKey
        private readonly ConcurrentDictionary<string, string> _importedFiles;

        // Locks for configuration operations
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _watcherLocks;

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="FileWatcher"/> class.
        /// </summary>
        public FileWatcher(
            IFileSystem fileSystem,
            IStoragePool storagePool,
            ITenantManager tenantManager,
            ILogger<FileWatcher> logger,
            string? configurationRoot = null)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _storagePool = storagePool ?? throw new ArgumentNullException(nameof(storagePool));
            _tenantManager = tenantManager ?? throw new ArgumentNullException(nameof(tenantManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _configurationRoot = configurationRoot ?? Path.Combine(".locus", "watchers");

            _importedFiles = new ConcurrentDictionary<string, string>();
            _watcherLocks = new ConcurrentDictionary<string, SemaphoreSlim>();

            // Ensure configuration directory exists
            if (!_fileSystem.Directory.Exists(_configurationRoot))
            {
                _fileSystem.Directory.CreateDirectory(_configurationRoot);
            }

            // Load imported files history from persistent storage
            LoadImportedFilesHistory();
        }

        /// <summary>
        /// Loads the imported files history from persistent storage to prevent re-importing files after restart.
        /// This is especially important for PostImportAction.Keep mode.
        /// </summary>
        private void LoadImportedFilesHistory()
        {
            try
            {
                var historyPath = _fileSystem.Path.Combine(_configurationRoot, "imported-files.json");
                if (_fileSystem.File.Exists(historyPath))
                {
                    var json = _fileSystem.File.ReadAllText(historyPath);
                    var history = JsonSerializer.Deserialize<Dictionary<string, string>>(json);

                    if (history != null)
                    {
                        foreach (var kvp in history)
                        {
                            _importedFiles.TryAdd(kvp.Key, kvp.Value);
                        }

                        _logger.LogInformation("Loaded {Count} imported file records from history", history.Count);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load imported files history, will start fresh");
            }
        }

        /// <summary>
        /// Saves the imported files history to persistent storage.
        /// </summary>
        private async Task SaveImportedFilesHistoryAsync()
        {
            try
            {
                var historyPath = _fileSystem.Path.Combine(_configurationRoot, "imported-files.json");
                var history = _importedFiles.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                using (var stream = _fileSystem.File.Create(historyPath))
                {
                    await JsonSerializer.SerializeAsync(stream, history, JsonOptions);
                }

                _logger.LogDebug("Saved imported files history: {Count} records", history.Count);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to save imported files history");
            }
        }

        /// <inheritdoc/>
        public async Task RegisterWatcherAsync(FileWatcherConfiguration configuration, CancellationToken ct)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            if (string.IsNullOrWhiteSpace(configuration.WatcherId))
                configuration.WatcherId = Guid.NewGuid().ToString("N");

            var watcherLock = _watcherLocks.GetOrAdd(configuration.WatcherId, _ => new SemaphoreSlim(1, 1));
            await watcherLock.WaitAsync(ct);

            try
            {
                // Validate tenant exists (only in single-tenant mode)
                if (!configuration.MultiTenantMode)
                {
                    if (string.IsNullOrWhiteSpace(configuration.TenantId))
                    {
                        throw new ArgumentException("TenantId is required in single-tenant mode.", nameof(configuration));
                    }

                    var tenant = await _tenantManager.GetTenantAsync(configuration.TenantId, ct);
                    if (tenant == null)
                    {
                        throw new InvalidOperationException($"Tenant '{configuration.TenantId}' not found.");
                    }
                }

                // Ensure watch path exists (auto-create if missing)
                if (!_fileSystem.Directory.Exists(configuration.WatchPath))
                {
                    _fileSystem.Directory.CreateDirectory(configuration.WatchPath);
                    _logger.LogInformation("Created watch path directory: {WatchPath}", configuration.WatchPath);
                }

                configuration.CreatedAt = DateTime.UtcNow;
                configuration.UpdatedAt = DateTime.UtcNow;

                await SaveConfigurationAsync(configuration, ct);

                _logger.LogInformation(
                    "Registered file watcher {WatcherId} for tenant {TenantId} at path {WatchPath}",
                    configuration.WatcherId, configuration.TenantId, configuration.WatchPath);
            }
            finally
            {
                watcherLock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task UpdateWatcherAsync(FileWatcherConfiguration configuration, CancellationToken ct)
        {
            var watcherLock = _watcherLocks.GetOrAdd(configuration.WatcherId, _ => new SemaphoreSlim(1, 1));
            await watcherLock.WaitAsync(ct);

            try
            {
                var existing = await LoadConfigurationAsync(configuration.WatcherId, ct);
                if (existing == null)
                {
                    throw new InvalidOperationException($"Watcher '{configuration.WatcherId}' not found.");
                }

                configuration.CreatedAt = existing.CreatedAt;
                configuration.UpdatedAt = DateTime.UtcNow;

                await SaveConfigurationAsync(configuration, ct);

                _logger.LogInformation("Updated file watcher {WatcherId}", configuration.WatcherId);
            }
            finally
            {
                watcherLock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task RemoveWatcherAsync(string watcherId, CancellationToken ct)
        {
            var watcherLock = _watcherLocks.GetOrAdd(watcherId, _ => new SemaphoreSlim(1, 1));
            await watcherLock.WaitAsync(ct);

            try
            {
                var configPath = GetConfigurationPath(watcherId);
                if (_fileSystem.File.Exists(configPath))
                {
                    _fileSystem.File.Delete(configPath);
                    _logger.LogInformation("Removed file watcher {WatcherId}", watcherId);
                }
            }
            finally
            {
                watcherLock.Release();
                _watcherLocks.TryRemove(watcherId, out _);
            }
        }

        /// <inheritdoc/>
        public async Task EnableWatcherAsync(string watcherId, CancellationToken ct)
        {
            await UpdateWatcherStatusAsync(watcherId, true, ct);
        }

        /// <inheritdoc/>
        public async Task DisableWatcherAsync(string watcherId, CancellationToken ct)
        {
            await UpdateWatcherStatusAsync(watcherId, false, ct);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<FileWatcherConfiguration>> GetWatchersForTenantAsync(string tenantId, CancellationToken ct)
        {
            var allWatchers = await GetAllWatchersAsync(ct);
            return allWatchers.Where(w => w.TenantId == tenantId);
        }

        /// <inheritdoc/>
        public async Task<FileWatcherConfiguration?> GetWatcherAsync(string watcherId, CancellationToken ct)
        {
            return await LoadConfigurationAsync(watcherId, ct);
        }

        /// <inheritdoc/>
        public async Task<FileWatcherScanResult> ScanNowAsync(string watcherId, CancellationToken ct)
        {
            var configuration = await LoadConfigurationAsync(watcherId, ct);
            if (configuration == null)
            {
                throw new InvalidOperationException($"Watcher '{watcherId}' not found.");
            }

            if (!configuration.Enabled)
            {
                throw new InvalidOperationException($"Watcher '{watcherId}' is disabled.");
            }

            return await ScanDirectoryAsync(configuration, ct);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<FileWatcherConfiguration>> GetAllWatchersAsync(CancellationToken ct)
        {
            var configFiles = _fileSystem.Directory.GetFiles(_configurationRoot, "*.json");
            var configurations = new List<FileWatcherConfiguration>();

            foreach (var filePath in configFiles)
            {
                try
                {
                    var fileName = _fileSystem.Path.GetFileNameWithoutExtension(filePath);
                    var config = await LoadConfigurationAsync(fileName, ct);
                    if (config != null)
                    {
                        configurations.Add(config);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to load watcher configuration from {FilePath}", filePath);
                }
            }

            return configurations;
        }

        private async Task<FileWatcherScanResult> ScanDirectoryAsync(FileWatcherConfiguration configuration, CancellationToken ct)
        {
            var result = new FileWatcherScanResult();

            try
            {
                if (configuration.MultiTenantMode)
                {
                    // Multi-tenant mode: each subdirectory is a tenant
                    await ScanMultiTenantDirectoryAsync(configuration, result, ct);
                }
                else
                {
                    // Single-tenant mode: all files belong to one tenant
                    await ScanSingleTenantDirectoryAsync(configuration, result, ct);
                }
            }
            catch (Exception ex)
            {
                result.Errors.Add($"Scan error: {ex.Message}");
                _logger.LogError(ex, "Error scanning directory for watcher {WatcherId}", configuration.WatcherId);
            }

            return result;
        }

        private async Task ScanSingleTenantDirectoryAsync(FileWatcherConfiguration configuration, FileWatcherScanResult result, CancellationToken ct)
        {
            var tenant = await _tenantManager.GetTenantAsync(configuration.TenantId, ct);

            // Get all files recursively
            var files = GetFilesRecursive(
                configuration.WatchPath,
                configuration.FilePatterns,
                configuration.IncludeSubdirectories);

            result.FilesDiscovered += files.Count;

            await ProcessFilesAsync(configuration, tenant, files, result, ct);
        }

        private async Task ScanMultiTenantDirectoryAsync(FileWatcherConfiguration configuration, FileWatcherScanResult result, CancellationToken ct)
        {
            // Auto-create tenant directories if enabled
            if (configuration.AutoCreateTenantDirectories)
            {
                try
                {
                    _logger.LogDebug("Auto-creating tenant directories in {WatchPath}", configuration.WatchPath);

                    // Get all tenants from the system
                    var allTenants = await _tenantManager.GetAllTenantsAsync(ct);

                    foreach (var tenant in allTenants)
                    {
                        var tenantDir = _fileSystem.Path.Combine(configuration.WatchPath, tenant.TenantId);
                        if (!_fileSystem.Directory.Exists(tenantDir))
                        {
                            _fileSystem.Directory.CreateDirectory(tenantDir);
                            _logger.LogInformation("Auto-created tenant directory: {TenantDirectory}", tenantDir);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to auto-create tenant directories in {WatchPath}", configuration.WatchPath);
                }
            }

            // Get all immediate subdirectories (each represents a tenant)
            var tenantDirectories = _fileSystem.Directory.GetDirectories(configuration.WatchPath);

            foreach (var tenantDirectory in tenantDirectories)
            {
                if (ct.IsCancellationRequested)
                    break;

                try
                {
                    // Extract tenant ID from directory name
                    var tenantId = _fileSystem.Path.GetFileName(tenantDirectory);

                    // Validate tenant exists
                    ITenantContext tenant;
                    try
                    {
                        tenant = await _tenantManager.GetTenantAsync(tenantId, ct);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Skipping directory {TenantDirectory} - tenant {TenantId} not found or disabled",
                            tenantDirectory, tenantId);
                        continue;
                    }

                    // Check if tenant is enabled
                    if (!await _tenantManager.IsTenantEnabledAsync(tenantId, ct))
                    {
                        _logger.LogDebug("Skipping directory {TenantDirectory} - tenant {TenantId} is disabled",
                            tenantDirectory, tenantId);
                        continue;
                    }

                    // Scan tenant directory recursively
                    var files = GetFilesRecursive(
                        tenantDirectory,
                        configuration.FilePatterns,
                        configuration.IncludeSubdirectories);

                    result.FilesDiscovered += files.Count;

                    _logger.LogDebug("Found {FileCount} files in tenant directory {TenantDirectory}",
                        files.Count, tenantDirectory);

                    await ProcessFilesAsync(configuration, tenant, files, result, ct);
                }
                catch (Exception ex)
                {
                    result.Errors.Add($"Error scanning tenant directory {tenantDirectory}: {ex.Message}");
                    _logger.LogError(ex, "Error scanning tenant directory {TenantDirectory}", tenantDirectory);
                }
            }
        }

        private async Task ProcessFilesAsync(
            FileWatcherConfiguration configuration,
            ITenantContext tenant,
            List<string> files,
            FileWatcherScanResult result,
            CancellationToken ct)
        {
            // Validate MaxConcurrentImports
            var maxConcurrency = Math.Max(1, configuration.MaxConcurrentImports);

            if (maxConcurrency == 1)
            {
                // Sequential processing (no concurrency)
                foreach (var filePath in files)
                {
                    if (ct.IsCancellationRequested)
                        break;

                    var fileResult = await ProcessSingleFileAsync(configuration, tenant, filePath, ct);
                    MergeResult(result, fileResult);
                }
            }
            else
            {
                // Concurrent processing with semaphore limiting
                using (var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency))
                {
                    var tasks = files.Select(async filePath =>
                    {
                        await semaphore.WaitAsync(ct);
                        try
                        {
                            return await ProcessSingleFileAsync(configuration, tenant, filePath, ct);
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    });

                    var results = await Task.WhenAll(tasks);

                    // Merge all results
                    foreach (var fileResult in results)
                    {
                        MergeResult(result, fileResult);
                    }
                }
            }
        }

        private async Task<FileWatcherScanResult> ProcessSingleFileAsync(
            FileWatcherConfiguration configuration,
            ITenantContext tenant,
            string filePath,
            CancellationToken ct)
        {
            var fileResult = new FileWatcherScanResult();

            try
            {
                if (ct.IsCancellationRequested)
                    return fileResult;

                // Skip if already imported
                if (_importedFiles.ContainsKey(filePath))
                {
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Get file info for validation
                var fileInfo = _fileSystem.FileInfo.New(filePath);

                // Skip empty files (0 bytes)
                if (fileInfo.Length == 0)
                {
                    _logger.LogDebug("Skipping empty file {FilePath} (0 bytes)", filePath);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Check file age (prevent importing files still being written)
                var lastWriteTime = _fileSystem.File.GetLastWriteTimeUtc(filePath);
                if (DateTime.UtcNow - lastWriteTime < configuration.MinFileAge)
                {
                    _logger.LogDebug("Skipping file {FilePath} - too recent (age: {Age})",
                        filePath, DateTime.UtcNow - lastWriteTime);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Check if file is still being written (multiple methods)
                if (!await IsFileReadyForImportAsync(filePath, ct))
                {
                    _logger.LogDebug("Skipping file {FilePath} - still being written", filePath);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Check file size limit
                if (configuration.MaxFileSizeBytes > 0 && fileInfo.Length > configuration.MaxFileSizeBytes)
                {
                    _logger.LogWarning(
                        "Skipping file {FilePath} - size {FileSize} exceeds limit {MaxSize}",
                        filePath, fileInfo.Length, configuration.MaxFileSizeBytes);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Import file
                using (var stream = _fileSystem.File.OpenRead(filePath))
                {
                    var fileKey = await _storagePool.WriteFileAsync(tenant, stream, ct);
                    _importedFiles[filePath] = fileKey;

                    fileResult.FilesImported++;
                    fileResult.BytesImported += fileInfo.Length;

                    _logger.LogInformation(
                        "Imported file {FilePath} as {FileKey} for tenant {TenantId}",
                        filePath, fileKey, tenant.TenantId);
                }

                // Save import history to prevent re-importing after restart (especially important for Keep mode)
                await SaveImportedFilesHistoryAsync();

                // Post-import action
                await ExecutePostImportActionAsync(configuration, filePath, ct);
            }
            catch (Exception ex)
            {
                fileResult.FilesFailed++;
                fileResult.Errors.Add($"{filePath}: {ex.Message}");
                _logger.LogError(ex, "Failed to import file {FilePath}", filePath);
            }

            return fileResult;
        }

        private void MergeResult(FileWatcherScanResult target, FileWatcherScanResult source)
        {
            target.FilesImported += source.FilesImported;
            target.FilesSkipped += source.FilesSkipped;
            target.FilesFailed += source.FilesFailed;
            target.BytesImported += source.BytesImported;
            target.Errors.AddRange(source.Errors);
        }

        private List<string> GetFilesRecursive(string path, List<string> patterns, bool includeSubdirectories)
        {
            var files = new List<string>();

            try
            {
                foreach (var pattern in patterns)
                {
                    var matchingFiles = _fileSystem.Directory.GetFiles(
                        path,
                        pattern,
                        includeSubdirectories ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly);

                    files.AddRange(matchingFiles);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error scanning directory {Path}", path);
            }

            return files.Distinct().ToList();
        }

        private async Task ExecutePostImportActionAsync(FileWatcherConfiguration configuration, string filePath, CancellationToken ct)
        {
            switch (configuration.PostImportAction)
            {
                case PostImportAction.Delete:
                    _fileSystem.File.Delete(filePath);
                    _logger.LogDebug("Deleted file {FilePath} after import", filePath);

                    // Remove from history since file no longer exists
                    _importedFiles.TryRemove(filePath, out _);
                    await SaveImportedFilesHistoryAsync();
                    break;

                case PostImportAction.Move:
                    if (!string.IsNullOrEmpty(configuration.MoveToDirectory))
                    {
                        var moveToDir = configuration.MoveToDirectory!;
                        if (!_fileSystem.Directory.Exists(moveToDir))
                        {
                            _fileSystem.Directory.CreateDirectory(moveToDir);
                        }

                        var fileName = _fileSystem.Path.GetFileName(filePath);
                        var targetPath = _fileSystem.Path.Combine(moveToDir, fileName);

                        // Handle duplicate filenames
                        var counter = 1;
                        while (_fileSystem.File.Exists(targetPath))
                        {
                            var nameWithoutExt = _fileSystem.Path.GetFileNameWithoutExtension(fileName);
                            var extension = _fileSystem.Path.GetExtension(fileName);
                            fileName = $"{nameWithoutExt}_{counter}{extension}";
                            targetPath = _fileSystem.Path.Combine(moveToDir, fileName);
                            counter++;
                        }

                        _fileSystem.File.Move(filePath, targetPath);
                        _logger.LogDebug("Moved file {FilePath} to {TargetPath}", filePath, targetPath);

                        // Remove from history since file is no longer in watch directory
                        _importedFiles.TryRemove(filePath, out _);
                        await SaveImportedFilesHistoryAsync();
                    }
                    break;

                case PostImportAction.Keep:
                    // Do nothing
                    break;
            }

            await Task.CompletedTask;
        }

        private async Task UpdateWatcherStatusAsync(string watcherId, bool enabled, CancellationToken ct)
        {
            var watcherLock = _watcherLocks.GetOrAdd(watcherId, _ => new SemaphoreSlim(1, 1));
            await watcherLock.WaitAsync(ct);

            try
            {
                var configuration = await LoadConfigurationAsync(watcherId, ct);
                if (configuration == null)
                {
                    throw new InvalidOperationException($"Watcher '{watcherId}' not found.");
                }

                configuration.Enabled = enabled;
                configuration.UpdatedAt = DateTime.UtcNow;

                await SaveConfigurationAsync(configuration, ct);

                _logger.LogInformation(
                    "Watcher {WatcherId} {Status}",
                    watcherId, enabled ? "enabled" : "disabled");
            }
            finally
            {
                watcherLock.Release();
            }
        }

        private async Task<FileWatcherConfiguration?> LoadConfigurationAsync(string watcherId, CancellationToken ct)
        {
            var configPath = GetConfigurationPath(watcherId);

            if (!_fileSystem.File.Exists(configPath))
            {
                return null;
            }

            try
            {
                using (var stream = _fileSystem.File.OpenRead(configPath))
                {
                    return await JsonSerializer.DeserializeAsync<FileWatcherConfiguration>(stream, JsonOptions, ct);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load watcher configuration {WatcherId}", watcherId);
                throw;
            }
        }

        private async Task SaveConfigurationAsync(FileWatcherConfiguration configuration, CancellationToken ct)
        {
            var configPath = GetConfigurationPath(configuration.WatcherId);

            using (var stream = _fileSystem.File.Create(configPath))
            {
                await JsonSerializer.SerializeAsync(stream, configuration, JsonOptions, ct);
            }
        }

        private string GetConfigurationPath(string watcherId)
        {
            return _fileSystem.Path.Combine(_configurationRoot, $"{watcherId}.json");
        }

        /// <summary>
        /// Check if file is ready for import by using multiple detection methods.
        /// </summary>
        private async Task<bool> IsFileReadyForImportAsync(string filePath, CancellationToken ct)
        {
            try
            {
                // Method 1: Try to open file exclusively
                // If file is being written, this will fail
                if (!IsFileAccessible(filePath))
                {
                    return false;
                }

                // Method 2: Check if file size is stable
                // Wait a short time and verify size hasn't changed
                var initialSize = _fileSystem.FileInfo.New(filePath).Length;
                var initialWriteTime = _fileSystem.File.GetLastWriteTimeUtc(filePath);

                // Wait 100ms
                await Task.Delay(100, ct);

                var finalSize = _fileSystem.FileInfo.New(filePath).Length;
                var finalWriteTime = _fileSystem.File.GetLastWriteTimeUtc(filePath);

                // If size or write time changed, file is still being written
                if (initialSize != finalSize || initialWriteTime != finalWriteTime)
                {
                    return false;
                }

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error checking file readiness for {FilePath}", filePath);
                return false;
            }
        }

        /// <summary>
        /// Check if file can be opened exclusively (not being used by another process).
        /// </summary>
        private bool IsFileAccessible(string filePath)
        {
            try
            {
                // Try to open file with exclusive access
                // If another process is writing, this will throw IOException
                using (var stream = _fileSystem.File.Open(
                    filePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.None))
                {
                    return true;
                }
            }
            catch (IOException)
            {
                // File is being used by another process
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                // No permission to access file
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error checking file accessibility for {FilePath}", filePath);
                return false;
            }
        }
    }
}
