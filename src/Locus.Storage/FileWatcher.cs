using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
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
        private readonly Channel<ImportedHistoryOperation> _pendingImportedHistoryChannel;
        private readonly ChannelWriter<ImportedHistoryOperation> _pendingImportedHistoryWriter;
        private readonly ChannelReader<ImportedHistoryOperation> _pendingImportedHistoryReader;
        private readonly ConcurrentDictionary<string, ImportedHistoryOperation> _coalescedImportedHistoryOperations;
        private readonly SemaphoreSlim _historySaveLock;
        private int _importedHistoryDirty;
        private int _pendingImportedHistoryDepth;
        private int _coalescedImportedHistoryDepth;
        private int _historyJournalOperationCount;
        private long _lastImportedHistoryFlushUtcTicks;
        private long _lastImportedFilesPruneUtcTicks;
        private int _pruneInProgress;
        private int _importedPruneCursor;
        private string[]? _importedPruneSnapshot;
        private readonly ConcurrentDictionary<string, PatternMatcherCacheEntry> _patternMatcherCache;
        private readonly ConcurrentDictionary<string, AutoCreateTenantDirectoryCacheEntry> _autoCreateTenantDirectoryCache;
        private readonly SemaphoreSlim _watcherCacheLock;
        private volatile IReadOnlyList<FileWatcherConfiguration>? _cachedWatchers;
        private long _watchersCacheExpiresAtTicks;

        // Prune stale entries when the cache exceeds this size to prevent unbounded growth in Keep mode.
        private const int MaxImportedFilesCacheSize = 10_000;
        private const int ImportedFilesPruneBudgetPerScan = 500;
        private const string InFlightImportMarker = "__LOCUS_IN_FLIGHT__";
        private const string ImportedFilesCheckpointFileName = "imported-files.json";
        private const string ImportedFilesJournalFileName = "imported-files.journal.jsonl";
        private const string ImportedFilesCheckpointTempFileName = "imported-files.tmp.json";
        private const int ImportedHistoryCompactThresholdOperationCount = 4_000;
        private const long ImportedHistoryCompactThresholdBytes = 4L * 1024 * 1024;
        private const int MaxImportedHistoryChannelSize = 20_000;
        private const int MinImportQueueCapacity = 64;
        private const int MaxImportQueueCapacity = 1024;
        private static readonly long WatchersCacheTtlTicks = TimeSpan.FromSeconds(2).Ticks;
        private static readonly TimeSpan DefaultAutoCreateTenantDirectoriesCacheTtl = TimeSpan.FromSeconds(60);
        private static readonly TimeSpan DefaultImportedHistoryPruneInterval = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan DefaultImportedHistoryFlushInterval = TimeSpan.FromSeconds(2);

        // Locks for configuration operations
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _watcherLocks;

        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        private static readonly JsonSerializerOptions HistoryJournalJsonOptions = new JsonSerializerOptions
        {
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
            _pendingImportedHistoryChannel = Channel.CreateBounded<ImportedHistoryOperation>(new BoundedChannelOptions(MaxImportedHistoryChannelSize)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            });
            _pendingImportedHistoryWriter = _pendingImportedHistoryChannel.Writer;
            _pendingImportedHistoryReader = _pendingImportedHistoryChannel.Reader;
            _coalescedImportedHistoryOperations = new ConcurrentDictionary<string, ImportedHistoryOperation>(StringComparer.Ordinal);
            _watcherLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
            _patternMatcherCache = new ConcurrentDictionary<string, PatternMatcherCacheEntry>(StringComparer.Ordinal);
            _autoCreateTenantDirectoryCache = new ConcurrentDictionary<string, AutoCreateTenantDirectoryCacheEntry>(StringComparer.Ordinal);
            _historySaveLock = new SemaphoreSlim(1, 1);
            _watcherCacheLock = new SemaphoreSlim(1, 1);

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
                var checkpoint = LoadImportedHistoryCheckpoint();
                var journalResult = ReplayImportedHistoryJournal(checkpoint);
                var loaded = 0;

                foreach (var kvp in checkpoint)
                {
                    // Skip entries whose source file no longer exists.
                    // This discards stale Delete/Move records that survived a crash
                    // (the file was deleted/moved but the history save happened before the journal delete op flushed).
                    if (_fileSystem.File.Exists(kvp.Key))
                    {
                        _importedFiles.TryAdd(kvp.Key, kvp.Value);
                        loaded++;
                    }
                }

                if (checkpoint.Count > 0 || journalResult.AppliedOperations > 0 || journalResult.InvalidLines > 0)
                {
                    _logger.LogInformation(
                        "Loaded {Loaded}/{Total} imported file records from history checkpoint + journal ({Skipped} stale, JournalOps={JournalOps}, InvalidJournalLines={InvalidLines})",
                        loaded,
                        checkpoint.Count,
                        checkpoint.Count - loaded,
                        journalResult.AppliedOperations,
                        journalResult.InvalidLines);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load imported files history, will start fresh");
            }
        }

        /// <summary>
        /// Removes entries from the in-memory cache whose source files no longer exist.
        /// Called when the cache exceeds MaxImportedFilesCacheSize to prevent unbounded growth in Keep mode.
        /// </summary>
        private void PruneStaleImportedEntries()
        {
            if (_importedFiles.Count == 0)
                return;

            var directoryExistsCache = new Dictionary<string, bool>(StringComparer.Ordinal);
            var snapshot = _importedPruneSnapshot;
            if (snapshot == null || snapshot.Length == 0 || _importedPruneCursor >= snapshot.Length)
            {
                snapshot = _importedFiles.Keys
                    .Where(path => !string.IsNullOrWhiteSpace(path))
                    .ToArray();
                _importedPruneSnapshot = snapshot;
                _importedPruneCursor = 0;
            }

            if (snapshot.Length == 0)
                return;

            int pruned = 0;
            var processed = 0;
            while (_importedPruneCursor < snapshot.Length && processed < ImportedFilesPruneBudgetPerScan)
            {
                var key = snapshot[_importedPruneCursor++];
                processed++;

                if (string.IsNullOrWhiteSpace(key))
                    continue;

                if (!_importedFiles.ContainsKey(key))
                    continue;

                if (IsImportedSourceMissing(key, directoryExistsCache))
                {
                    if (TryRemoveImportedFileRecord(key))
                        pruned++;
                }
            }

            if (_importedPruneCursor >= snapshot.Length)
            {
                _importedPruneSnapshot = null;
                _importedPruneCursor = 0;
            }

            if (pruned > 0)
            {
                _logger.LogInformation(
                    "Pruned {Count} stale imported file entries (processed {Processed} of {Total} this round)",
                    pruned,
                    processed,
                    snapshot.Length);
            }
        }

        private bool IsImportedSourceMissing(string sourcePath, Dictionary<string, bool> directoryExistsCache)
        {
            try
            {
                var directoryPath = _fileSystem.Path.GetDirectoryName(sourcePath);
                if (!string.IsNullOrWhiteSpace(directoryPath))
                {
                    var directoryKey = directoryPath!;
                    if (!directoryExistsCache.TryGetValue(directoryKey, out var directoryExists))
                    {
                        directoryExists = _fileSystem.Directory.Exists(directoryPath);
                        directoryExistsCache[directoryKey] = directoryExists;
                    }

                    if (!directoryExists)
                        return true;
                }

                return !_fileSystem.File.Exists(sourcePath);
            }
            catch
            {
                // Fail-open: keep the record if the path probe throws transient IO exceptions.
                return false;
            }
        }

        private void MarkImportedFilesHistoryDirty()
        {
            Interlocked.Exchange(ref _importedHistoryDirty, 1);
        }

        private void EnqueueImportedHistoryOperation(ImportedHistoryOperation operation)
        {
            if (!TryWriteImportedHistoryOperation(operation))
            {
                var added = _coalescedImportedHistoryOperations.TryAdd(operation.Path, operation);
                if (!added)
                    _coalescedImportedHistoryOperations[operation.Path] = operation;

                if (added)
                    Interlocked.Increment(ref _coalescedImportedHistoryDepth);
            }

            MarkImportedFilesHistoryDirty();
        }

        private bool TryWriteImportedHistoryOperation(ImportedHistoryOperation operation)
        {
            Interlocked.Increment(ref _pendingImportedHistoryDepth);
            if (_pendingImportedHistoryWriter.TryWrite(operation))
                return true;

            Interlocked.Decrement(ref _pendingImportedHistoryDepth);
            return false;
        }

        /// <summary>
        /// Saves the imported files history to persistent storage.
        /// </summary>
        private async Task SaveImportedFilesHistoryAsync(bool force, CancellationToken ct)
        {
            var lockTaken = false;
            List<ImportedHistoryOperation>? pendingOperations = null;
            try
            {
                await _historySaveLock.WaitAsync(ct).ConfigureAwait(false);
                lockTaken = true;

                if (Volatile.Read(ref _importedHistoryDirty) == 0
                    && Volatile.Read(ref _pendingImportedHistoryDepth) == 0
                    && Volatile.Read(ref _coalescedImportedHistoryDepth) == 0)
                    return;

                // Claim the current dirty set before snapshotting so concurrent updates can
                // re-mark dirty and trigger a follow-up flush with newer state.
                Interlocked.Exchange(ref _importedHistoryDirty, 0);

                pendingOperations = DrainImportedHistoryOperations();
                var appendedOperations = 0;
                if (pendingOperations.Count > 0)
                    appendedOperations = await AppendImportedHistoryJournalAsync(pendingOperations, ct).ConfigureAwait(false);

                if (ShouldCompactImportedHistory(force))
                {
                    await CompactImportedHistoryAsync(ct).ConfigureAwait(false);
                }

                Interlocked.Exchange(ref _lastImportedHistoryFlushUtcTicks, DateTime.UtcNow.Ticks);

                if (Volatile.Read(ref _pendingImportedHistoryDepth) > 0
                    || Volatile.Read(ref _coalescedImportedHistoryDepth) > 0)
                    MarkImportedFilesHistoryDirty();

                if (appendedOperations > 0)
                {
                    _logger.LogDebug(
                        "Flushed imported files history journal operations: {Count} (pending: {Pending})",
                        appendedOperations,
                        Volatile.Read(ref _pendingImportedHistoryDepth));
                }
            }
            catch (Exception ex)
            {
                if (pendingOperations != null && pendingOperations.Count > 0)
                    RequeueImportedHistoryOperations(pendingOperations);

                MarkImportedFilesHistoryDirty();
                _logger.LogWarning(ex, "Failed to save imported files history");
            }
            finally
            {
                if (lockTaken)
                    _historySaveLock.Release();
            }
        }

        private async Task FlushImportedFilesHistoryIfDirtyAsync(
            FileWatcherConfiguration configuration,
            bool force,
            CancellationToken ct)
        {
            if (Volatile.Read(ref _importedHistoryDirty) == 0)
                return;

            if (!force && configuration.EnableImportedFilesHistoryFlushDebounce)
            {
                var flushInterval = NormalizeInterval(
                    configuration.ImportedFilesHistoryFlushInterval,
                    DefaultImportedHistoryFlushInterval);
                var lastFlushTicks = Interlocked.Read(ref _lastImportedHistoryFlushUtcTicks);
                if (lastFlushTicks > 0)
                {
                    var elapsedSinceFlush = DateTime.UtcNow - new DateTime(lastFlushTicks, DateTimeKind.Utc);
                    if (elapsedSinceFlush < flushInterval)
                        return;
                }
            }

            await SaveImportedFilesHistoryAsync(force, ct).ConfigureAwait(false);
        }

        private static TimeSpan NormalizeInterval(TimeSpan configured, TimeSpan fallback)
        {
            return configured <= TimeSpan.Zero ? fallback : configured;
        }

        private Dictionary<string, string> LoadImportedHistoryCheckpoint()
        {
            var checkpointPath = GetImportedHistoryCheckpointPath();
            if (!_fileSystem.File.Exists(checkpointPath))
                return new Dictionary<string, string>();

            var json = _fileSystem.File.ReadAllText(checkpointPath);
            return JsonSerializer.Deserialize<Dictionary<string, string>>(json)
                ?? new Dictionary<string, string>();
        }

        private (int AppliedOperations, int InvalidLines) ReplayImportedHistoryJournal(
            Dictionary<string, string> checkpointState)
        {
            var journalPath = GetImportedHistoryJournalPath();
            if (!_fileSystem.File.Exists(journalPath))
            {
                Interlocked.Exchange(ref _historyJournalOperationCount, 0);
                return (0, 0);
            }

            var applied = 0;
            var invalid = 0;
            using (var stream = _fileSystem.File.OpenRead(journalPath))
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    ImportedHistoryJournalEntry? entry;
                    try
                    {
                        entry = JsonSerializer.Deserialize<ImportedHistoryJournalEntry>(line, HistoryJournalJsonOptions);
                    }
                    catch
                    {
                        invalid++;
                        continue;
                    }

                    if (entry == null || string.IsNullOrWhiteSpace(entry.Path))
                    {
                        invalid++;
                        continue;
                    }

                    switch (entry.Operation)
                    {
                        case ImportedHistoryOperationType.Delete:
                            checkpointState.Remove(entry.Path);
                            applied++;
                            break;
                        case ImportedHistoryOperationType.Upsert:
                            if (string.IsNullOrWhiteSpace(entry.FileKey))
                            {
                                invalid++;
                                continue;
                            }

                            checkpointState[entry.Path] = entry.FileKey!;
                            applied++;
                            break;
                        default:
                            invalid++;
                            break;
                    }
                }
            }

            Interlocked.Exchange(ref _historyJournalOperationCount, applied);
            return (applied, invalid);
        }

        private void UpsertImportedFileRecord(string path, string fileKey)
        {
            _importedFiles[path] = fileKey;
            EnqueueImportedHistoryOperation(ImportedHistoryOperation.CreateUpsert(path, fileKey));
        }

        private bool TryRemoveImportedFileRecord(string path)
        {
            if (!_importedFiles.TryRemove(path, out var removedFileKey))
                return false;

            if (string.Equals(removedFileKey, InFlightImportMarker, StringComparison.Ordinal))
                return true;

            EnqueueImportedHistoryOperation(ImportedHistoryOperation.CreateDelete(path));
            return true;
        }

        private List<ImportedHistoryOperation> DrainImportedHistoryOperations()
        {
            var operations = new List<ImportedHistoryOperation>();
            var channelDepthSnapshot = Volatile.Read(ref _pendingImportedHistoryDepth);
            for (var i = 0; i < channelDepthSnapshot; i++)
            {
                if (!_pendingImportedHistoryReader.TryRead(out var operation))
                    break;

                operations.Add(operation);
                Interlocked.Decrement(ref _pendingImportedHistoryDepth);
            }

            if (Volatile.Read(ref _coalescedImportedHistoryDepth) > 0)
            {
                foreach (var kvp in _coalescedImportedHistoryOperations)
                {
                    if (!_coalescedImportedHistoryOperations.TryRemove(kvp.Key, out var operation))
                        continue;

                    operations.Add(operation);
                    Interlocked.Decrement(ref _coalescedImportedHistoryDepth);
                }
            }

            return operations;
        }

        private void RequeueImportedHistoryOperations(List<ImportedHistoryOperation> operations)
        {
            foreach (var operation in operations)
                EnqueueImportedHistoryOperation(operation);
        }

        private async Task<int> AppendImportedHistoryJournalAsync(
            List<ImportedHistoryOperation> operations,
            CancellationToken ct)
        {
            if (operations.Count == 0)
                return 0;

            var journalPath = GetImportedHistoryJournalPath();

            using (var stream = _fileSystem.File.Open(journalPath, FileMode.Append, FileAccess.Write, FileShare.Read))
            using (var writer = new StreamWriter(stream))
            {
                foreach (var operation in operations)
                {
                    ct.ThrowIfCancellationRequested();
                    var entry = new ImportedHistoryJournalEntry
                    {
                        Operation = operation.Operation,
                        Path = operation.Path,
                        FileKey = operation.FileKey
                    };

                    var line = JsonSerializer.Serialize(entry, HistoryJournalJsonOptions);
                    await writer.WriteLineAsync(line).ConfigureAwait(false);
                }

                await writer.FlushAsync().ConfigureAwait(false);
            }

            Interlocked.Add(ref _historyJournalOperationCount, operations.Count);
            return operations.Count;
        }

        private bool ShouldCompactImportedHistory(bool force)
        {
            if (force)
                return true;

            if (Interlocked.CompareExchange(ref _historyJournalOperationCount, 0, 0)
                >= ImportedHistoryCompactThresholdOperationCount)
            {
                return true;
            }

            var journalPath = GetImportedHistoryJournalPath();
            if (!_fileSystem.File.Exists(journalPath))
                return false;

            try
            {
                var length = _fileSystem.FileInfo.New(journalPath).Length;
                return length >= ImportedHistoryCompactThresholdBytes;
            }
            catch
            {
                return false;
            }
        }

        private async Task CompactImportedHistoryAsync(CancellationToken ct)
        {
            var checkpointPath = GetImportedHistoryCheckpointPath();
            var tempPath = GetImportedHistoryCheckpointTempPath();
            var journalPath = GetImportedHistoryJournalPath();

            try
            {
                using (var stream = _fileSystem.File.Create(tempPath))
                {
                    // Stream checkpoint JSON directly to avoid allocating a large intermediate
                    // dictionary when imported history contains many entries.
                    using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = JsonOptions.WriteIndented }))
                    {
                        writer.WriteStartObject();
                        foreach (var kvp in _importedFiles)
                        {
                            if (string.Equals(kvp.Value, InFlightImportMarker, StringComparison.Ordinal))
                                continue;

                            writer.WriteString(kvp.Key, kvp.Value);
                        }

                        writer.WriteEndObject();
                        writer.Flush();
                    }
                }

                if (_fileSystem.File.Exists(checkpointPath))
                    _fileSystem.File.Delete(checkpointPath);
                _fileSystem.File.Move(tempPath, checkpointPath);

                if (_fileSystem.File.Exists(journalPath))
                    _fileSystem.File.Delete(journalPath);

                Interlocked.Exchange(ref _historyJournalOperationCount, 0);
                _logger.LogDebug("Compacted imported files history checkpoint");
            }
            catch
            {
                if (_fileSystem.File.Exists(tempPath))
                {
                    try
                    {
                        _fileSystem.File.Delete(tempPath);
                    }
                    catch
                    {
                        // Ignore cleanup failures for temporary checkpoint file.
                    }
                }

                throw;
            }
        }

        private string GetImportedHistoryCheckpointPath()
        {
            return _fileSystem.Path.Combine(_configurationRoot, ImportedFilesCheckpointFileName);
        }

        private string GetImportedHistoryJournalPath()
        {
            return _fileSystem.Path.Combine(_configurationRoot, ImportedFilesJournalFileName);
        }

        private string GetImportedHistoryCheckpointTempPath()
        {
            return _fileSystem.Path.Combine(_configurationRoot, ImportedFilesCheckpointTempFileName);
        }

        private void TryPruneStaleImportedEntries(FileWatcherConfiguration configuration)
        {
            if (_importedFiles.Count < MaxImportedFilesCacheSize)
                return;

            if (configuration.EnableImportedFilesPruneThrottle)
            {
                var pruneInterval = NormalizeInterval(
                    configuration.ImportedFilesPruneInterval,
                    DefaultImportedHistoryPruneInterval);
                var lastPruneTicks = Interlocked.Read(ref _lastImportedFilesPruneUtcTicks);
                if (lastPruneTicks > 0)
                {
                    var elapsedSincePrune = DateTime.UtcNow - new DateTime(lastPruneTicks, DateTimeKind.Utc);
                    if (elapsedSincePrune < pruneInterval)
                        return;
                }
            }

            if (Interlocked.CompareExchange(ref _pruneInProgress, 1, 0) != 0)
                return;

            try
            {
                Interlocked.Exchange(ref _lastImportedFilesPruneUtcTicks, DateTime.UtcNow.Ticks);
                PruneStaleImportedEntries();
            }
            finally
            {
                Volatile.Write(ref _pruneInProgress, 0);
            }
        }

        private void InvalidateWatchersCache()
        {
            _cachedWatchers = null;
            Interlocked.Exchange(ref _watchersCacheExpiresAtTicks, 0);
        }

        private void InvalidatePatternMatcherCache(string watcherId)
        {
            if (string.IsNullOrWhiteSpace(watcherId))
                return;

            _patternMatcherCache.TryRemove(watcherId, out _);
        }

        private void InvalidateAutoCreateTenantDirectoryCache(string watcherId)
        {
            if (string.IsNullOrWhiteSpace(watcherId))
                return;

            _autoCreateTenantDirectoryCache.TryRemove(watcherId, out _);
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
                InvalidateAutoCreateTenantDirectoryCache(configuration.WatcherId);

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
                InvalidateAutoCreateTenantDirectoryCache(configuration.WatcherId);

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
                    InvalidateWatchersCache();
                    InvalidatePatternMatcherCache(watcherId);
                    InvalidateAutoCreateTenantDirectoryCache(watcherId);
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
            // Fast path for BackgroundFileWatcherService: it calls GetAllWatchersAsync first,
            // so we can reuse the same in-memory snapshot and avoid a second file read.
            var configuration = TryGetCachedWatcherConfiguration(watcherId);
            if (configuration == null)
                configuration = await LoadConfigurationAsync(watcherId, ct);

            if (configuration == null)
            {
                throw new InvalidOperationException($"Watcher '{watcherId}' not found.");
            }

            return await ScanNowAsync(configuration, ct);
        }

        /// <inheritdoc/>
        public async Task<FileWatcherScanResult> ScanNowAsync(FileWatcherConfiguration configuration, CancellationToken ct)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var watcherId = string.IsNullOrWhiteSpace(configuration.WatcherId)
                ? "<unknown>"
                : configuration.WatcherId;

            if (!configuration.Enabled)
                throw new InvalidOperationException($"Watcher '{watcherId}' is disabled.");

            return await ScanDirectoryAsync(configuration, ct);
        }

        private FileWatcherConfiguration? TryGetCachedWatcherConfiguration(string watcherId)
        {
            if (string.IsNullOrWhiteSpace(watcherId))
                return null;

            var cached = _cachedWatchers;
            if (cached == null)
                return null;

            var nowTicks = DateTime.UtcNow.Ticks;
            if (nowTicks >= Interlocked.Read(ref _watchersCacheExpiresAtTicks))
                return null;

            for (var i = 0; i < cached.Count; i++)
            {
                var candidate = cached[i];
                if (string.Equals(candidate.WatcherId, watcherId, StringComparison.Ordinal))
                    return candidate;
            }

            return null;
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<FileWatcherConfiguration>> GetAllWatchersAsync(CancellationToken ct)
        {
            var nowTicks = DateTime.UtcNow.Ticks;
            var cached = _cachedWatchers;
            if (cached != null && nowTicks < Interlocked.Read(ref _watchersCacheExpiresAtTicks))
                return cached;

            await _watcherCacheLock.WaitAsync(ct);
            try
            {
                cached = _cachedWatchers;
                if (cached != null && DateTime.UtcNow.Ticks < Interlocked.Read(ref _watchersCacheExpiresAtTicks))
                    return cached;

                var configFiles = _fileSystem.Directory.GetFiles(_configurationRoot, "*.json");
                var configurations = new List<FileWatcherConfiguration>();

                foreach (var filePath in configFiles)
                {
                    try
                    {
                        var fileName = _fileSystem.Path.GetFileNameWithoutExtension(filePath);
                        if (fileName.StartsWith("imported-files", StringComparison.OrdinalIgnoreCase))
                            continue;

                        var config = await LoadConfigurationAsync(fileName, ct);
                        if (config != null)
                            configurations.Add(config);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to load watcher configuration from {FilePath}", filePath);
                    }
                }

                var snapshot = configurations.ToArray();
                _cachedWatchers = snapshot;
                Interlocked.Exchange(ref _watchersCacheExpiresAtTicks, DateTime.UtcNow.AddTicks(WatchersCacheTtlTicks).Ticks);
                return snapshot;
            }
            finally
            {
                _watcherCacheLock.Release();
            }
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
                configuration.WatcherId,
                configuration.WatchPath,
                configuration.FilePatterns,
                configuration.IncludeSubdirectories);

            var discovered = await ProcessFilesAsync(configuration, tenant, files, result, ct);
            result.FilesDiscovered += discovered;
        }

        private async Task ScanMultiTenantDirectoryAsync(FileWatcherConfiguration configuration, FileWatcherScanResult result, CancellationToken ct)
        {
            var tenantDirectories = _fileSystem.Directory.GetDirectories(configuration.WatchPath);

            // Auto-create tenant directories if enabled
            if (configuration.AutoCreateTenantDirectories)
            {
                try
                {
                    tenantDirectories = await EnsureTenantDirectoriesAsync(configuration, tenantDirectories, ct);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to auto-create tenant directories in {WatchPath}", configuration.WatchPath);
                }
            }

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

                    // Reuse the status from GetTenantAsync to avoid a second tenant lookup.
                    // Fallback to IsTenantEnabledAsync only when Status is an unknown value
                    // (e.g. mocked ITenantContext that does not set Status).
                    if (tenant.Status != TenantStatus.Enabled)
                    {
                        if (tenant.Status == TenantStatus.Disabled || tenant.Status == TenantStatus.Suspended)
                        {
                            _logger.LogDebug("Skipping directory {TenantDirectory} - tenant {TenantId} is disabled",
                                tenantDirectory, tenantId);
                            continue;
                        }

                        if (!await _tenantManager.IsTenantEnabledAsync(tenantId, ct))
                        {
                            _logger.LogDebug("Skipping directory {TenantDirectory} - tenant {TenantId} is disabled",
                                tenantDirectory, tenantId);
                            continue;
                        }
                    }

                    // Scan tenant directory recursively
                    var files = GetFilesRecursive(
                        configuration.WatcherId,
                        tenantDirectory,
                        configuration.FilePatterns,
                        configuration.IncludeSubdirectories);

                    var discovered = await ProcessFilesAsync(configuration, tenant, files, result, ct);
                    result.FilesDiscovered += discovered;

                    _logger.LogDebug("Found {FileCount} files in tenant directory {TenantDirectory}",
                        discovered, tenantDirectory);
                }
                catch (Exception ex)
                {
                    result.Errors.Add($"Error scanning tenant directory {tenantDirectory}: {ex.Message}");
                    _logger.LogError(ex, "Error scanning tenant directory {TenantDirectory}", tenantDirectory);
                }
            }
        }

        private async Task<string[]> EnsureTenantDirectoriesAsync(
            FileWatcherConfiguration configuration,
            string[] existingDirectories,
            CancellationToken ct)
        {
            _logger.LogDebug("Auto-creating tenant directories in {WatchPath}", configuration.WatchPath);

            var tenants = await GetTenantsForAutoCreateAsync(configuration, ct);
            var mergedDirectories = existingDirectories.ToList();
            var existingTenantIds = new HashSet<string>(
                existingDirectories.Select(path => _fileSystem.Path.GetFileName(path)),
                StringComparer.OrdinalIgnoreCase);

            foreach (var tenantId in tenants)
            {
                if (existingTenantIds.Contains(tenantId))
                    continue;

                var tenantDir = _fileSystem.Path.Combine(configuration.WatchPath, tenantId);
                _fileSystem.Directory.CreateDirectory(tenantDir);
                mergedDirectories.Add(tenantDir);
                existingTenantIds.Add(tenantId);

                _logger.LogInformation("Auto-created tenant directory: {TenantDirectory}", tenantDir);
            }

            return mergedDirectories.ToArray();
        }

        private async Task<string[]> GetTenantsForAutoCreateAsync(
            FileWatcherConfiguration configuration,
            CancellationToken ct)
        {
            var watcherId = string.IsNullOrWhiteSpace(configuration.WatcherId)
                ? configuration.WatchPath
                : configuration.WatcherId;

            var now = DateTime.UtcNow;
            var cacheTtl = NormalizeInterval(
                configuration.AutoCreateTenantDirectoriesCacheTtl,
                DefaultAutoCreateTenantDirectoriesCacheTtl);

            if (_autoCreateTenantDirectoryCache.TryGetValue(watcherId, out var cached)
                && now < cached.ExpiresAtUtc)
            {
                return cached.TenantIds;
            }

            var allTenants = await _tenantManager.GetAllTenantsAsync(ct);
            var tenantIds = allTenants
                .Select(tenant => tenant.TenantId)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToArray();

            _autoCreateTenantDirectoryCache[watcherId] = new AutoCreateTenantDirectoryCacheEntry(
                tenantIds,
                now.Add(cacheTtl));

            return tenantIds;
        }

        private async Task<int> ProcessFilesAsync(
            FileWatcherConfiguration configuration,
            ITenantContext tenant,
            IEnumerable<string> files,
            FileWatcherScanResult result,
            CancellationToken ct)
        {
            // Validate MaxConcurrentImports
            var maxConcurrency = Math.Max(1, configuration.MaxConcurrentImports);
            var discovered = 0;

            try
            {
                if (maxConcurrency == 1)
                {
                    // Sequential processing keeps memory flat by streaming file enumeration.
                    foreach (var filePath in files)
                    {
                        if (ct.IsCancellationRequested)
                            break;

                        discovered++;
                        var fileResult = await ProcessSingleFileAsync(configuration, tenant, filePath, ct);
                        MergeResult(result, fileResult);
                    }
                }
                else
                {
                    // Concurrent processing with a fixed-size queue keeps memory bounded even when
                    // directory scans return very large file sets.
                    var queueCapacity = Math.Min(
                        MaxImportQueueCapacity,
                        Math.Max(MinImportQueueCapacity, maxConcurrency * 4));
                    var queue = Channel.CreateBounded<string>(new BoundedChannelOptions(queueCapacity)
                    {
                        SingleWriter = true,
                        SingleReader = false,
                        FullMode = BoundedChannelFullMode.Wait,
                        AllowSynchronousContinuations = false
                    });
                    var workers = Enumerable.Range(0, maxConcurrency).Select(async _ =>
                    {
                        var workerResult = new FileWatcherScanResult();
                        while (!ct.IsCancellationRequested && await queue.Reader.WaitToReadAsync(ct))
                        {
                            if (!queue.Reader.TryRead(out var filePath))
                                continue;

                            var fileResult = await ProcessSingleFileAsync(configuration, tenant, filePath, ct);
                            MergeResult(workerResult, fileResult);
                        }
                        return workerResult;
                    }).ToArray();
                    Exception? producerException = null;
                    FileWatcherScanResult[] workerResults = Array.Empty<FileWatcherScanResult>();
                    try
                    {
                        foreach (var filePath in files)
                        {
                            ct.ThrowIfCancellationRequested();
                            discovered++;
                            if (!queue.Writer.TryWrite(filePath))
                                await queue.Writer.WriteAsync(filePath, ct);
                        }
                    }
                    catch (Exception ex)
                    {
                        producerException = ex;
                    }
                    finally
                    {
                        queue.Writer.TryComplete();
                        try
                        {
                            workerResults = await Task.WhenAll(workers);
                        }
                        catch (OperationCanceledException) when (ct.IsCancellationRequested)
                        {
                            if (producerException == null)
                                producerException = new OperationCanceledException(ct);
                        }
                    }

                    foreach (var workerResult in workerResults)
                    {
                        MergeResult(result, workerResult);
                    }

                    if (producerException != null)
                        ExceptionDispatchInfo.Capture(producerException).Throw();
                }
            }
            finally
            {
                // Run stale-history pruning once per scan batch instead of once per file
                // to keep file-import hot path free of O(n) File.Exists sweeps.
                TryPruneStaleImportedEntries(configuration);
                await FlushImportedFilesHistoryIfDirtyAsync(configuration, force: false, ct).ConfigureAwait(false);
            }

            return discovered;
        }

        private async Task<FileWatcherScanResult> ProcessSingleFileAsync(
            FileWatcherConfiguration configuration,
            ITenantContext tenant,
            string filePath,
            CancellationToken ct)
        {
            var fileResult = new FileWatcherScanResult();
            var importSlotTaken = false;

            try
            {
                if (ct.IsCancellationRequested)
                    return fileResult;

                // Acquire import slot atomically to prevent duplicate imports under concurrent scans.
                if (!_importedFiles.TryAdd(filePath, InFlightImportMarker))
                {
                    fileResult.FilesSkipped++;
                    return fileResult;
                }
                importSlotTaken = true;

                // Get file info for validation (single stat object reused by stability probes).
                var fileInfo = _fileSystem.FileInfo.New(filePath);
                var fileSize = fileInfo.Length;
                var lastWriteTime = fileInfo.LastWriteTimeUtc;

                // Skip empty files (0 bytes)
                if (fileSize == 0)
                {
                    _logger.LogDebug("Skipping empty file {FilePath} (0 bytes)", filePath);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Check file age (prevent importing files still being written)
                var fileAge = DateTime.UtcNow - lastWriteTime;
                if (fileAge < configuration.MinFileAge)
                {
                    _logger.LogDebug("Skipping file {FilePath} - too recent (age: {Age})",
                        filePath, fileAge);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Check if file is still being written (multiple methods)
                if (!await IsFileReadyForImportAsync(filePath, fileInfo, fileAge, configuration, ct))
                {
                    _logger.LogDebug("Skipping file {FilePath} - still being written", filePath);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                var importStream = TryOpenImportStream(filePath);
                if (importStream == null)
                {
                    _logger.LogDebug("Skipping file {FilePath} - cannot acquire exclusive read access", filePath);
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                var importSize = importStream.Length;
                if (configuration.MaxFileSizeBytes > 0 && importSize > configuration.MaxFileSizeBytes)
                {
                    _logger.LogWarning(
                        "Skipping file {FilePath} - size {FileSize} exceeds limit {MaxSize}",
                        filePath, importSize, configuration.MaxFileSizeBytes);
                    importStream.Dispose();
                    fileResult.FilesSkipped++;
                    return fileResult;
                }

                // Import file
                using (importStream)
                {
                    var fileName = Path.GetFileName(filePath);
                    var fileKey = await _storagePool.WriteFileAsync(tenant, importStream, fileName, ct);
                    UpsertImportedFileRecord(filePath, fileKey);
                    importSlotTaken = false;

                    fileResult.FilesImported++;
                    fileResult.BytesImported += importSize;

                    _logger.LogInformation(
                        "Imported file {FilePath} as {FileKey} for tenant {TenantId}",
                        filePath, fileKey, tenant.TenantId);
                }

                // Post-import action
                await ExecutePostImportActionAsync(configuration, filePath, ct);
            }
            catch (Exception ex)
            {
                fileResult.FilesFailed++;
                fileResult.Errors.Add($"{filePath}: {ex.Message}");
                _logger.LogError(ex, "Failed to import file {FilePath}", filePath);
            }
            finally
            {
                // Release import slot if the import was not completed successfully.
                // importSlotTaken is set to false after successful import (line 1284),
                // so this block only runs when the import failed or was skipped.
                if (importSlotTaken)
                {
                    // Only remove if the value is still the in-flight marker.
                    // Another thread might have updated it concurrently (unlikely but possible).
                    if (_importedFiles.TryGetValue(filePath, out var currentValue)
                        && string.Equals(currentValue, InFlightImportMarker, StringComparison.Ordinal))
                    {
                        _importedFiles.TryRemove(filePath, out _);
                    }
                    // Note: If the value is not the in-flight marker, it means another thread
                    // has already updated the record (e.g., a concurrent scan imported the file).
                    // In this case, we don't remove the record to avoid breaking the other import.
                }
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

        private IEnumerable<string> GetFilesRecursive(
            string watcherId,
            string path,
            List<string> patterns,
            bool includeSubdirectories)
        {
            List<string> normalizedPatterns;

            try
            {
                normalizedPatterns = (patterns ?? new List<string> { "*" })
                    .Where(p => !string.IsNullOrWhiteSpace(p))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                if (normalizedPatterns.Count == 0)
                    normalizedPatterns.Add("*");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error scanning directory {Path}", path);
                return Enumerable.Empty<string>();
            }

            var searchOption = includeSubdirectories ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly;
            if (normalizedPatterns.Count == 1 && normalizedPatterns[0] == "*")
                return EnumerateFilesSafe(path, searchOption);

            var matcherCacheEntry = GetOrCreatePatternMatcherCacheEntry(
                ResolvePatternMatcherCacheKey(watcherId, path),
                normalizedPatterns);
            return EnumerateMatchingFiles(path, searchOption, matcherCacheEntry.Matchers);
        }

        private static string ResolvePatternMatcherCacheKey(string watcherId, string path)
        {
            if (!string.IsNullOrWhiteSpace(watcherId))
                return watcherId;

            // ScanNowAsync(configuration) may be called with an ad-hoc configuration that does
            // not have a persisted WatcherId yet. Use watch path as a stable in-memory cache key.
            return string.IsNullOrWhiteSpace(path) ? "<anonymous-watcher>" : path;
        }

        private IEnumerable<string> EnumerateFilesSafe(string path, SearchOption searchOption)
        {
            IEnumerator<string>? enumerator;
            try
            {
                enumerator = _fileSystem.Directory.EnumerateFiles(path, "*", searchOption).GetEnumerator();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error scanning directory {Path}", path);
                return Enumerable.Empty<string>();
            }

            return EnumerateFiles(path, enumerator);
        }

        private IEnumerable<string> EnumerateMatchingFiles(string path, SearchOption searchOption, Regex[] matchers)
        {
            IEnumerator<string>? enumerator;
            try
            {
                enumerator = _fileSystem.Directory.EnumerateFiles(path, "*", searchOption).GetEnumerator();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error scanning directory {Path}", path);
                return Enumerable.Empty<string>();
            }

            return EnumerateFiles(path, enumerator, matchers);
        }

        private IEnumerable<string> EnumerateFiles(string path, IEnumerator<string> enumerator, Regex[]? matchers = null)
        {
            using (enumerator)
            {
                while (true)
                {
                    string file;
                    try
                    {
                        if (!enumerator.MoveNext())
                            yield break;

                        file = enumerator.Current;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error scanning directory {Path}", path);
                        yield break;
                    }

                    if (matchers == null)
                    {
                        yield return file;
                        continue;
                    }

                    var fileName = _fileSystem.Path.GetFileName(file);
                    if (MatchesAnyPattern(fileName, matchers))
                        yield return file;
                }
            }
        }

        private static bool MatchesAnyPattern(string fileName, Regex[] matchers)
        {
            for (var i = 0; i < matchers.Length; i++)
            {
                if (matchers[i].IsMatch(fileName))
                    return true;
            }

            return false;
        }

        private static Regex CreateWildcardMatcher(string pattern)
        {
            var normalized = string.IsNullOrWhiteSpace(pattern) ? "*" : pattern.Trim();
            var regexPattern = "^" + Regex.Escape(normalized)
                .Replace("\\*", ".*")
                .Replace("\\?", ".") + "$";

            return new Regex(regexPattern, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
        }

        private PatternMatcherCacheEntry GetOrCreatePatternMatcherCacheEntry(
            string watcherId,
            List<string> normalizedPatterns)
        {
            var fingerprint = string.Join(
                "\u001F",
                normalizedPatterns.Select(p => p.Trim().ToUpperInvariant()));

            if (_patternMatcherCache.TryGetValue(watcherId, out var existing)
                && string.Equals(existing.PatternFingerprint, fingerprint, StringComparison.Ordinal))
            {
                return existing;
            }

            var matchers = normalizedPatterns
                .Select(CreateWildcardMatcher)
                .ToArray();
            var created = new PatternMatcherCacheEntry(fingerprint, matchers);
            _patternMatcherCache[watcherId] = created;
            return created;
        }

        private Task ExecutePostImportActionAsync(FileWatcherConfiguration configuration, string filePath, CancellationToken ct)
        {
            switch (configuration.PostImportAction)
            {
                case PostImportAction.Delete:
                    _fileSystem.File.Delete(filePath);
                    _logger.LogDebug("Deleted file {FilePath} after import", filePath);

                    // Remove from history since file no longer exists
                    TryRemoveImportedFileRecord(filePath);
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
                        TryRemoveImportedFileRecord(filePath);
                    }
                    break;

                case PostImportAction.Keep:
                    // Do nothing
                    break;
            }

            return Task.CompletedTask;
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

            InvalidateWatchersCache();
            InvalidatePatternMatcherCache(configuration.WatcherId);
            InvalidateAutoCreateTenantDirectoryCache(configuration.WatcherId);
        }

        private string GetConfigurationPath(string watcherId)
        {
            return _fileSystem.Path.Combine(_configurationRoot, $"{watcherId}.json");
        }

        private readonly struct AutoCreateTenantDirectoryCacheEntry
        {
            public AutoCreateTenantDirectoryCacheEntry(string[] tenantIds, DateTime expiresAtUtc)
            {
                TenantIds = tenantIds;
                ExpiresAtUtc = expiresAtUtc;
            }

            public string[] TenantIds { get; }

            public DateTime ExpiresAtUtc { get; }
        }

        private readonly struct PatternMatcherCacheEntry
        {
            public PatternMatcherCacheEntry(string patternFingerprint, Regex[] matchers)
            {
                PatternFingerprint = patternFingerprint;
                Matchers = matchers;
            }

            public string PatternFingerprint { get; }

            public Regex[] Matchers { get; }
        }

        private readonly struct ImportedHistoryOperation
        {
            public ImportedHistoryOperation(ImportedHistoryOperationType operation, string path, string? fileKey)
            {
                Operation = operation;
                Path = path;
                FileKey = fileKey;
            }

            public ImportedHistoryOperationType Operation { get; }

            public string Path { get; }

            public string? FileKey { get; }

            public static ImportedHistoryOperation CreateUpsert(string path, string fileKey)
            {
                return new ImportedHistoryOperation(ImportedHistoryOperationType.Upsert, path, fileKey);
            }

            public static ImportedHistoryOperation CreateDelete(string path)
            {
                return new ImportedHistoryOperation(ImportedHistoryOperationType.Delete, path, null);
            }
        }

        private sealed class ImportedHistoryJournalEntry
        {
            public ImportedHistoryOperationType Operation { get; set; }

            public string Path { get; set; } = string.Empty;

            public string? FileKey { get; set; }
        }

        private enum ImportedHistoryOperationType
        {
            Upsert = 1,
            Delete = 2
        }

        /// <summary>
        /// Check if file is ready for import by using multiple detection methods.
        /// </summary>
        private async Task<bool> IsFileReadyForImportAsync(
            string filePath,
            IFileInfo fileInfo,
            TimeSpan fileAge,
            FileWatcherConfiguration configuration,
            CancellationToken ct)
        {
            try
            {
                // Old files are unlikely to still be mutating; skip delayed probes to improve throughput.
                if (fileAge >= configuration.SkipStabilityCheckAfterAge)
                    return true;

                var stabilityDelay = configuration.FileStabilityCheckDelay;
                if (stabilityDelay <= TimeSpan.Zero)
                    return true;

                // Check if file size/write-time remain stable across a short delay.
                // Wait a short time and verify size hasn't changed
                var initialSize = fileInfo.Length;
                var initialWriteTime = fileInfo.LastWriteTimeUtc;

                await Task.Delay(stabilityDelay, ct);

                fileInfo.Refresh();
                var finalSize = fileInfo.Length;
                var finalWriteTime = fileInfo.LastWriteTimeUtc;

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

        private Stream? TryOpenImportStream(string filePath)
        {
            try
            {
                return _fileSystem.File.Open(
                    filePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.None);
            }
            catch (IOException)
            {
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error opening file for import {FilePath}", filePath);
                return null;
            }
        }
    }
}
