using System;
using System.IO;
using System.IO.Abstractions;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Manages global file watcher service configuration with file-based persistence.
    /// </summary>
    public class FileWatcherOptionsManager : IFileWatcherOptionsManager
    {
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<FileWatcherOptionsManager> _logger;
        private readonly string _configurationRoot;
        private readonly SemaphoreSlim _lock;
        private FileWatcherOptions? _cachedOptions;

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="FileWatcherOptionsManager"/> class.
        /// </summary>
        public FileWatcherOptionsManager(
            IFileSystem fileSystem,
            ILogger<FileWatcherOptionsManager> logger,
            string? configurationRoot = null)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _configurationRoot = configurationRoot ?? Path.Combine(".locus", "config");
            _lock = new SemaphoreSlim(1, 1);

            // Ensure configuration directory exists
            if (!_fileSystem.Directory.Exists(_configurationRoot))
            {
                _fileSystem.Directory.CreateDirectory(_configurationRoot);
            }
        }

        /// <inheritdoc/>
        public async Task<FileWatcherOptions> GetOptionsAsync(CancellationToken ct)
        {
            // Return cached options if available
            if (_cachedOptions != null)
            {
                return _cachedOptions;
            }

            await _lock.WaitAsync(ct);
            try
            {
                // Double-check after acquiring lock
                if (_cachedOptions != null)
                {
                    return _cachedOptions;
                }

                var options = await LoadOptionsAsync(ct);
                _cachedOptions = options;
                return options;
            }
            finally
            {
                _lock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task UpdateOptionsAsync(FileWatcherOptions options, CancellationToken ct)
        {
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            // Validate intervals
            if (options.DefaultPollingInterval < options.MinimumPollingInterval)
            {
                throw new ArgumentException(
                    $"DefaultPollingInterval ({options.DefaultPollingInterval}) cannot be less than MinimumPollingInterval ({options.MinimumPollingInterval})");
            }

            if (options.DefaultPollingInterval > options.MaximumPollingInterval)
            {
                throw new ArgumentException(
                    $"DefaultPollingInterval ({options.DefaultPollingInterval}) cannot be greater than MaximumPollingInterval ({options.MaximumPollingInterval})");
            }

            await _lock.WaitAsync(ct);
            try
            {
                await SaveOptionsAsync(options, ct);
                _cachedOptions = options;
                _logger.LogInformation("File watcher options updated. Enabled: {Enabled}", options.Enabled);
            }
            finally
            {
                _lock.Release();
            }
        }

        /// <inheritdoc/>
        public async Task EnableServiceAsync(CancellationToken ct)
        {
            var options = await GetOptionsAsync(ct);
            if (options.Enabled)
            {
                _logger.LogDebug("File watcher service is already enabled");
                return;
            }

            options.Enabled = true;
            await UpdateOptionsAsync(options, ct);
            _logger.LogInformation("File watcher service enabled");
        }

        /// <inheritdoc/>
        public async Task DisableServiceAsync(CancellationToken ct)
        {
            var options = await GetOptionsAsync(ct);
            if (!options.Enabled)
            {
                _logger.LogDebug("File watcher service is already disabled");
                return;
            }

            options.Enabled = false;
            await UpdateOptionsAsync(options, ct);
            _logger.LogInformation("File watcher service disabled");
        }

        /// <inheritdoc/>
        public async Task<bool> IsServiceEnabledAsync(CancellationToken ct)
        {
            var options = await GetOptionsAsync(ct);
            return options.Enabled;
        }

        private async Task<FileWatcherOptions> LoadOptionsAsync(CancellationToken ct)
        {
            var configPath = GetConfigurationPath();

            if (!_fileSystem.File.Exists(configPath))
            {
                _logger.LogDebug("File watcher options file not found, creating default configuration");
                var defaultOptions = new FileWatcherOptions();
                await SaveOptionsAsync(defaultOptions, ct);
                return defaultOptions;
            }

            try
            {
                using (var stream = _fileSystem.File.OpenRead(configPath))
                {
                    var options = await JsonSerializer.DeserializeAsync<FileWatcherOptions>(stream, JsonOptions, ct);
                    return options ?? new FileWatcherOptions();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load file watcher options, using defaults");
                return new FileWatcherOptions();
            }
        }

        private async Task SaveOptionsAsync(FileWatcherOptions options, CancellationToken ct)
        {
            var configPath = GetConfigurationPath();

            using (var stream = _fileSystem.File.Create(configPath))
            {
                await JsonSerializer.SerializeAsync(stream, options, JsonOptions, ct);
            }
        }

        private string GetConfigurationPath()
        {
            return _fileSystem.Path.Combine(_configurationRoot, "file-watcher-options.json");
        }
    }
}
