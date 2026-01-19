using System;
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
    /// Automatically manages file watchers based on root configuration.
    /// </summary>
    public class FileWatcherAutoManager : IFileWatcherAutoManager
    {
        private readonly IFileSystem _fileSystem;
        private readonly IFileWatcher _fileWatcher;
        private readonly ITenantManager _tenantManager;
        private readonly ILogger<FileWatcherAutoManager> _logger;
        private readonly string _configurationRoot;
        private readonly SemaphoreSlim _lock;
        private FileWatcherRootConfiguration? _currentRootConfig;
        private readonly HashSet<string> _managedWatcherIds;

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="FileWatcherAutoManager"/> class.
        /// </summary>
        public FileWatcherAutoManager(
            IFileSystem fileSystem,
            IFileWatcher fileWatcher,
            ITenantManager tenantManager,
            ILogger<FileWatcherAutoManager> logger,
            string? configurationRoot = null)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _fileWatcher = fileWatcher ?? throw new ArgumentNullException(nameof(fileWatcher));
            _tenantManager = tenantManager ?? throw new ArgumentNullException(nameof(tenantManager));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _configurationRoot = configurationRoot ?? Path.Combine(".locus", "auto-manager");
            _lock = new SemaphoreSlim(1, 1);
            _managedWatcherIds = new HashSet<string>();

            // Ensure configuration directory exists
            if (!_fileSystem.Directory.Exists(_configurationRoot))
            {
                _fileSystem.Directory.CreateDirectory(_configurationRoot);
            }
        }

        /// <inheritdoc/>
        public async Task<int> ApplyRootConfigurationAsync(FileWatcherRootConfiguration rootConfig, CancellationToken ct)
        {
            if (rootConfig == null)
                throw new ArgumentNullException(nameof(rootConfig));

            if (string.IsNullOrWhiteSpace(rootConfig.RootPath))
                throw new ArgumentException("RootPath is required", nameof(rootConfig));

            if (!_fileSystem.Directory.Exists(rootConfig.RootPath))
                throw new DirectoryNotFoundException($"Root path '{rootConfig.RootPath}' does not exist");

            await _lock.WaitAsync(ct);
            try
            {
                _currentRootConfig = rootConfig;
                await SaveRootConfigurationAsync(rootConfig, ct);

                int watchersCreated;

                if (rootConfig.MultiTenantMode)
                {
                    _logger.LogInformation("Applying multi-tenant root configuration for path: {RootPath}", rootConfig.RootPath);
                    watchersCreated = await CreateMultiTenantWatchersAsync(rootConfig, ct);
                }
                else
                {
                    _logger.LogInformation("Applying single-tenant root configuration for path: {RootPath}", rootConfig.RootPath);
                    watchersCreated = await CreateSingleTenantWatcherAsync(rootConfig, ct);
                }

                _logger.LogInformation("Applied root configuration: {WatchersCreated} watchers created/updated", watchersCreated);
                return watchersCreated;
            }
            finally
            {
                _lock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<int> DiscoverAndCreateWatchersAsync(CancellationToken ct)
        {
            await _lock.WaitAsync(ct);
            try
            {
                if (_currentRootConfig == null)
                {
                    _logger.LogWarning("No root configuration set, cannot discover tenants");
                    return 0;
                }

                if (!_currentRootConfig.MultiTenantMode)
                {
                    _logger.LogDebug("Multi-tenant mode is disabled, skipping discovery");
                    return 0;
                }

                if (!_currentRootConfig.AutoDiscoverTenants)
                {
                    _logger.LogDebug("Auto-discovery is disabled, skipping discovery");
                    return 0;
                }

                _logger.LogInformation("Discovering new tenant directories in: {RootPath}", _currentRootConfig.RootPath);
                return await CreateMultiTenantWatchersAsync(_currentRootConfig, ct);
            }
            finally
            {
                _lock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task RemoveAllWatchersAsync(CancellationToken ct)
        {
            await _lock.WaitAsync(ct);
            try
            {
                _logger.LogInformation("Removing all managed watchers ({Count} total)", _managedWatcherIds.Count);

                foreach (var watcherId in _managedWatcherIds.ToList())
                {
                    try
                    {
                        await _fileWatcher.RemoveWatcherAsync(watcherId, ct);
                        _logger.LogDebug("Removed watcher: {WatcherId}", watcherId);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to remove watcher: {WatcherId}", watcherId);
                    }
                }

                _managedWatcherIds.Clear();
                _currentRootConfig = null;

                // Clean up root config file
                var configPath = GetRootConfigurationPath();
                if (_fileSystem.File.Exists(configPath))
                {
                    _fileSystem.File.Delete(configPath);
                }
            }
            finally
            {
                _lock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<FileWatcherRootConfiguration?> GetRootConfigurationAsync(CancellationToken ct)
        {
            if (_currentRootConfig != null)
            {
                return _currentRootConfig;
            }

            await _lock.WaitAsync(ct);
            try
            {
                _currentRootConfig = await LoadRootConfigurationAsync(ct);
                return _currentRootConfig;
            }
            finally
            {
                _lock.Release();
            }
        }

        private async Task<int> CreateMultiTenantWatchersAsync(FileWatcherRootConfiguration rootConfig, CancellationToken ct)
        {
            // Single watcher in multi-tenant mode
            var watcherId = GenerateWatcherId("multi-tenant", rootConfig.RootPath);

            var watcherConfig = new FileWatcherConfiguration
            {
                WatcherId = watcherId,
                WatchPath = rootConfig.RootPath,
                MultiTenantMode = true,
                AutoCreateTenantDirectories = rootConfig.AutoCreateTenantDirectories,
                Enabled = rootConfig.Enabled,
                IncludeSubdirectories = rootConfig.IncludeSubdirectories,
                FilePatterns = rootConfig.FilePatterns,
                PostImportAction = rootConfig.PostImportAction,
                MoveToDirectory = rootConfig.MoveToDirectory,
                PollingInterval = rootConfig.PollingInterval,
                MaxFileSizeBytes = rootConfig.MaxFileSizeBytes,
                MinFileAge = rootConfig.MinFileAge,
                MaxConcurrentImports = rootConfig.MaxConcurrentImports
            };

            // Check if watcher already exists
            var existing = await _fileWatcher.GetWatcherAsync(watcherId, ct);
            if (existing != null)
            {
                _logger.LogDebug("Updating existing multi-tenant watcher: {WatcherId}", watcherId);
                await _fileWatcher.UpdateWatcherAsync(watcherConfig, ct);
            }
            else
            {
                _logger.LogInformation("Creating new multi-tenant watcher: {WatcherId}", watcherId);
                await _fileWatcher.RegisterWatcherAsync(watcherConfig, ct);
            }

            _managedWatcherIds.Add(watcherId);
            return 1;
        }

        private async Task<int> CreateSingleTenantWatcherAsync(FileWatcherRootConfiguration rootConfig, CancellationToken ct)
        {
            // In single-tenant mode, monitor only the root directory
            // The tenant ID should be provided separately or default to "default"
            var tenantId = "default";

            // Try to get tenant, if it doesn't exist, create it
            try
            {
                await _tenantManager.GetTenantAsync(tenantId, ct);
            }
            catch
            {
                _logger.LogInformation("Creating default tenant for single-tenant mode");
                await _tenantManager.CreateTenantAsync(tenantId, ct);
            }

            var watcherId = GenerateWatcherId("single-tenant", rootConfig.RootPath);

            var watcherConfig = new FileWatcherConfiguration
            {
                WatcherId = watcherId,
                TenantId = tenantId,
                WatchPath = rootConfig.RootPath,
                MultiTenantMode = false,
                Enabled = rootConfig.Enabled,
                IncludeSubdirectories = rootConfig.IncludeSubdirectories,
                FilePatterns = rootConfig.FilePatterns,
                PostImportAction = rootConfig.PostImportAction,
                MoveToDirectory = rootConfig.MoveToDirectory,
                PollingInterval = rootConfig.PollingInterval,
                MaxFileSizeBytes = rootConfig.MaxFileSizeBytes,
                MinFileAge = rootConfig.MinFileAge,
                MaxConcurrentImports = rootConfig.MaxConcurrentImports
            };

            // Check if watcher already exists
            var existing = await _fileWatcher.GetWatcherAsync(watcherId, ct);
            if (existing != null)
            {
                _logger.LogDebug("Updating existing single-tenant watcher: {WatcherId}", watcherId);
                await _fileWatcher.UpdateWatcherAsync(watcherConfig, ct);
            }
            else
            {
                _logger.LogInformation("Creating new single-tenant watcher: {WatcherId}", watcherId);
                await _fileWatcher.RegisterWatcherAsync(watcherConfig, ct);
            }

            _managedWatcherIds.Add(watcherId);
            return 1;
        }

        private string GenerateWatcherId(string mode, string rootPath)
        {
            // Create deterministic ID based on mode and path
            var normalizedPath = rootPath.Replace("\\", "/").TrimEnd('/');
            var hash = normalizedPath.GetHashCode().ToString("X8");
            return $"auto-{mode}-{hash}";
        }

        private async Task SaveRootConfigurationAsync(FileWatcherRootConfiguration config, CancellationToken ct)
        {
            var configPath = GetRootConfigurationPath();

            using (var stream = _fileSystem.File.Create(configPath))
            {
                await JsonSerializer.SerializeAsync(stream, config, JsonOptions, ct);
            }
        }

        private async Task<FileWatcherRootConfiguration?> LoadRootConfigurationAsync(CancellationToken ct)
        {
            var configPath = GetRootConfigurationPath();

            if (!_fileSystem.File.Exists(configPath))
            {
                return null;
            }

            try
            {
                using (var stream = _fileSystem.File.OpenRead(configPath))
                {
                    return await JsonSerializer.DeserializeAsync<FileWatcherRootConfiguration>(stream, JsonOptions, ct);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load root configuration");
                return null;
            }
        }

        private string GetRootConfigurationPath()
        {
            return _fileSystem.Path.Combine(_configurationRoot, "root-config.json");
        }
    }
}
