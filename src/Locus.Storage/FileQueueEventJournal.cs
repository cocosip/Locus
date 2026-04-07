using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Stores durable queue events as per-tenant JSONL logs.
    /// </summary>
    public class FileQueueEventJournal : IQueueEventJournal
    {
        private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };

        private readonly IFileSystem _fileSystem;
        private readonly ILogger<FileQueueEventJournal> _logger;
        private readonly string _queueDirectory;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _appendLocks;
        private readonly ConcurrentDictionary<string, JournalState> _stateCache;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileQueueEventJournal"/> class.
        /// </summary>
        public FileQueueEventJournal(
            IFileSystem fileSystem,
            ILogger<FileQueueEventJournal> logger,
            string queueDirectory)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            if (string.IsNullOrWhiteSpace(queueDirectory))
                throw new ArgumentException("Queue directory cannot be empty", nameof(queueDirectory));

            _queueDirectory = queueDirectory;
            _appendLocks = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);
            _stateCache = new ConcurrentDictionary<string, JournalState>(StringComparer.Ordinal);

            if (!_fileSystem.Directory.Exists(_queueDirectory))
                _fileSystem.Directory.CreateDirectory(_queueDirectory);
        }

        /// <inheritdoc/>
        public async Task AppendAsync(QueueEventRecord record, CancellationToken ct = default)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            if (string.IsNullOrWhiteSpace(record.TenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(record));

            if (string.IsNullOrWhiteSpace(record.FileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(record));

            var appendLock = _appendLocks.GetOrAdd(record.TenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var tenantDirectory = GetTenantDirectory(record.TenantId);
                if (!_fileSystem.Directory.Exists(tenantDirectory))
                    _fileSystem.Directory.CreateDirectory(tenantDirectory);

                var path = GetJournalPath(record.TenantId);
                var payload = JsonSerializer.Serialize(record, JsonOptions) + "\n";
                var bytes = Utf8NoBom.GetBytes(payload);

                using (var stream = _fileSystem.File.Open(path, FileMode.Append, FileAccess.Write, FileShare.Read))
                {
                    await stream.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
                    await stream.FlushAsync(ct).ConfigureAwait(false);
                }

                var state = LoadJournalState(record.TenantId);
                state.TailOffset += bytes.Length;
                SaveJournalState(record.TenantId, state);
            }
            finally
            {
                appendLock.Release();
            }
        }

        /// <inheritdoc/>
        public Task<IReadOnlyList<string>> GetTenantIdsAsync(CancellationToken ct = default)
        {
            ct.ThrowIfCancellationRequested();

            if (!_fileSystem.Directory.Exists(_queueDirectory))
                return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

            var tenantIds = _fileSystem.Directory
                .EnumerateDirectories(_queueDirectory)
                .Select(path => _fileSystem.Path.GetFileName(path))
                .Where(name => !string.IsNullOrWhiteSpace(name))
                .Distinct(StringComparer.Ordinal)
                .OrderBy(name => name, StringComparer.Ordinal)
                .ToArray();

            return Task.FromResult<IReadOnlyList<string>>(tenantIds);
        }

        /// <inheritdoc/>
        public async Task<QueueEventReadBatch> ReadBatchAsync(
            string tenantId,
            long offset,
            int maxRecords,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset cannot be negative.");

            if (maxRecords <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxRecords), "MaxRecords must be greater than zero.");

            var state = LoadJournalState(tenantId);
            var path = GetJournalPath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return new QueueEventReadBatch(Array.Empty<QueueEventRecord>(), state.BaseOffset, true);

            var records = new List<QueueEventRecord>(maxRecords);
            var effectiveOffset = offset < state.BaseOffset ? state.BaseOffset : offset;
            var nextRelativeOffset = effectiveOffset - state.BaseOffset;
            var reachedEndOfFile = true;

            using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                if (nextRelativeOffset > stream.Length)
                    nextRelativeOffset = stream.Length;

                stream.Seek(nextRelativeOffset, SeekOrigin.Begin);
                using (var reader = new StreamReader(stream, Utf8NoBom, detectEncodingFromByteOrderMarks: true, bufferSize: 16 * 1024, leaveOpen: true))
                {
                    while (records.Count < maxRecords)
                    {
                        ct.ThrowIfCancellationRequested();

                        var line = await reader.ReadLineAsync().ConfigureAwait(false);
                        if (line == null)
                            break;

                        nextRelativeOffset += Utf8NoBom.GetByteCount(line) + 1;
                        if (line.Length == 0)
                            continue;

                        try
                        {
                            var record = JsonSerializer.Deserialize<QueueEventRecord>(line, JsonOptions);
                            if (record != null)
                                records.Add(record);
                        }
                        catch (JsonException ex)
                        {
                            _logger.LogWarning(
                                ex,
                                "Skipping invalid queue journal line for tenant {TenantId} at offset {Offset}",
                                tenantId,
                                state.BaseOffset + nextRelativeOffset);
                        }
                    }
                }

                reachedEndOfFile = nextRelativeOffset >= stream.Length;
            }

            return new QueueEventReadBatch(records, state.BaseOffset + nextRelativeOffset, reachedEndOfFile);
        }

        /// <inheritdoc/>
        public async Task<long> CompactAsync(string tenantId, long processedOffset, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (processedOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(processedOffset), "Processed offset cannot be negative.");

            var appendLock = _appendLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var state = LoadJournalState(tenantId);
                var path = GetJournalPath(tenantId);
                if (!_fileSystem.File.Exists(path))
                    return state.BaseOffset;

                var fileInfo = _fileSystem.FileInfo.New(path);
                var length = fileInfo.Length;
                var relativeProcessedOffset = processedOffset - state.BaseOffset;
                if (relativeProcessedOffset <= 0 || relativeProcessedOffset > length)
                    return relativeProcessedOffset > length ? state.BaseOffset + length : state.BaseOffset;

                if (relativeProcessedOffset == length)
                {
                    using (var truncateStream = _fileSystem.File.Open(path, FileMode.Create, FileAccess.Write, FileShare.Read))
                    {
                        await truncateStream.FlushAsync(ct).ConfigureAwait(false);
                    }

                    state.BaseOffset = processedOffset;
                    state.TailOffset = processedOffset;
                    SaveJournalState(tenantId, state);
                    return processedOffset;
                }

                var tempPath = path + ".compact";
                using (var source = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                using (var destination = _fileSystem.File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    source.Seek(relativeProcessedOffset, SeekOrigin.Begin);
                    await source.CopyToAsync(destination, 16 * 1024, ct).ConfigureAwait(false);
                    await destination.FlushAsync(ct).ConfigureAwait(false);
                }

                _fileSystem.File.Delete(path);
                _fileSystem.File.Move(tempPath, path);
                state.BaseOffset = processedOffset;
                state.TailOffset = processedOffset + (length - relativeProcessedOffset);
                SaveJournalState(tenantId, state);
                return processedOffset;
            }
            finally
            {
                appendLock.Release();
            }
        }

        /// <inheritdoc/>
        public Task<long> GetTailOffsetAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            ct.ThrowIfCancellationRequested();
            var state = LoadJournalState(tenantId);
            return Task.FromResult(state.TailOffset);
        }

        /// <inheritdoc/>
        public Task<long> GetBaseOffsetAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            ct.ThrowIfCancellationRequested();
            var state = LoadJournalState(tenantId);
            return Task.FromResult(state.BaseOffset);
        }

        private string GetTenantDirectory(string tenantId)
        {
            return _fileSystem.Path.Combine(_queueDirectory, tenantId);
        }

        private string GetJournalPath(string tenantId)
        {
            return _fileSystem.Path.Combine(GetTenantDirectory(tenantId), "queue.log");
        }

        private string GetJournalStatePath(string tenantId)
        {
            return _fileSystem.Path.Combine(GetTenantDirectory(tenantId), "queue.state.json");
        }

        private JournalState LoadJournalState(string tenantId)
        {
            return _stateCache.GetOrAdd(tenantId, LoadJournalStateFromDisk);
        }

        private JournalState LoadJournalStateFromDisk(string tenantId)
        {
            var path = GetJournalStatePath(tenantId);
            if (_fileSystem.File.Exists(path))
            {
                try
                {
                    using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        var state = JsonSerializer.Deserialize<JournalState>(stream, JsonOptions);
                        if (state != null)
                        {
                            NormalizeJournalState(tenantId, state);
                            return state;
                        }
                    }
                }
                catch (Exception ex) when (ex is IOException || ex is JsonException || ex is UnauthorizedAccessException)
                {
                    _logger.LogWarning(ex, "Failed to load journal state for tenant {TenantId}", tenantId);
                }
            }

            var fallback = new JournalState();
            NormalizeJournalState(tenantId, fallback);
            return fallback;
        }

        private void SaveJournalState(string tenantId, JournalState state)
        {
            NormalizeJournalState(tenantId, state);
            var tenantDirectory = GetTenantDirectory(tenantId);
            if (!_fileSystem.Directory.Exists(tenantDirectory))
                _fileSystem.Directory.CreateDirectory(tenantDirectory);

            var path = GetJournalStatePath(tenantId);
            var tempPath = path + ".tmp";
            using (var stream = _fileSystem.File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
            {
                JsonSerializer.Serialize(stream, state, JsonOptions);
                stream.Flush();
            }

            if (_fileSystem.File.Exists(path))
                _fileSystem.File.Delete(path);

            _fileSystem.File.Move(tempPath, path);
            _stateCache[tenantId] = state;
        }

        private void NormalizeJournalState(string tenantId, JournalState state)
        {
            if (state.BaseOffset < 0)
                state.BaseOffset = 0;

            var path = GetJournalPath(tenantId);
            var fileLength = _fileSystem.File.Exists(path)
                ? _fileSystem.FileInfo.New(path).Length
                : 0;

            if (state.TailOffset < state.BaseOffset)
                state.TailOffset = state.BaseOffset;

            var minimumTailOffset = state.BaseOffset + fileLength;
            if (state.TailOffset < minimumTailOffset)
                state.TailOffset = minimumTailOffset;
        }

        private sealed class JournalState
        {
            public long BaseOffset { get; set; }

            public long TailOffset { get; set; }
        }
    }
}
