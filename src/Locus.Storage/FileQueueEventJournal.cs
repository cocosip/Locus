using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.IO;
using Locus.Core.Models;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Stores durable queue events as per-tenant journals with configurable codecs.
    /// </summary>
    public class FileQueueEventJournal : IQueueEventJournal, IDisposable
    {
        private static readonly EventId CorruptTailReadEventId = new EventId(4101, nameof(CorruptTailReadEventId));
        private static readonly EventId CorruptTailTruncatedEventId = new EventId(4102, nameof(CorruptTailTruncatedEventId));
        private static readonly EventId CorruptTailStateScanEventId = new EventId(4103, nameof(CorruptTailStateScanEventId));
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };
        private static readonly TimeSpan DefaultStateFlushDebounce = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan QueuePollInterval = TimeSpan.FromMilliseconds(1);

        private readonly IFileSystem _fileSystem;
        private readonly ILogger<FileQueueEventJournal> _logger;
        private readonly string _queueDirectory;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _appendLocks;
        private readonly ConcurrentDictionary<string, JournalState> _stateCache;
        private readonly ConcurrentDictionary<string, byte> _dirtyStateTenants;
        private readonly ConcurrentDictionary<string, TenantWriterState> _tenantWriters;
        private readonly Timer? _stateFlushTimer;
        private readonly TimeSpan _stateFlushDebounce;
        private readonly TimeSpan _linger;
        private readonly TimeSpan _writerIdleTimeout;
        private readonly TimeSpan _balancedFlushWindow;
        private readonly int _maxBatchRecords;
        private readonly int _maxBatchBytes;
        private readonly QueueEventJournalAckMode _ackMode;
        private readonly JournalFormat _defaultFormat;
        private readonly IQueueEventJournalCodec _jsonCodec;
        private readonly IQueueEventJournalCodec _binaryCodec;
        private int _stateFlushInProgress;
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileQueueEventJournal"/> class.
        /// </summary>
        public FileQueueEventJournal(
            IFileSystem fileSystem,
            ILogger<FileQueueEventJournal> logger,
            string queueDirectory,
            TimeSpan? stateFlushDebounce = null)
            : this(
                fileSystem,
                logger,
                new QueueEventJournalOptions
                {
                    QueueDirectory = queueDirectory,
                    StateFlushDebounce = stateFlushDebounce ?? DefaultStateFlushDebounce,
                })
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileQueueEventJournal"/> class.
        /// </summary>
        public FileQueueEventJournal(
            IFileSystem fileSystem,
            ILogger<FileQueueEventJournal> logger,
            QueueEventJournalOptions options)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            if (options == null)
                throw new ArgumentNullException(nameof(options));

            options.Validate();

            _queueDirectory = options.QueueDirectory;
            _appendLocks = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);
            _stateCache = new ConcurrentDictionary<string, JournalState>(StringComparer.Ordinal);
            _dirtyStateTenants = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
            _tenantWriters = new ConcurrentDictionary<string, TenantWriterState>(StringComparer.Ordinal);
            _stateFlushDebounce = options.StateFlushDebounce;
            _linger = options.Linger;
            _writerIdleTimeout = options.WriterIdleTimeout;
            _balancedFlushWindow = options.BalancedFlushWindow;
            _maxBatchRecords = options.MaxBatchRecords;
            _maxBatchBytes = options.MaxBatchBytes;
            _ackMode = options.AckMode;
            _defaultFormat = options.JournalFormat;
            _jsonCodec = new JsonQueueEventJournalCodec();
            _binaryCodec = new BinaryQueueEventJournalCodec();

            if (!_fileSystem.Directory.Exists(_queueDirectory))
                _fileSystem.Directory.CreateDirectory(_queueDirectory);

            if (_stateFlushDebounce > TimeSpan.Zero)
                _stateFlushTimer = new Timer(OnStateFlushTimer, null, Timeout.Infinite, Timeout.Infinite);
        }

        /// <inheritdoc/>
        public Task AppendAsync(QueueEventRecord record, CancellationToken ct = default)
        {
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            return AppendBatchAsync(new[] { record }, ct);
        }

        /// <inheritdoc/>
        public async Task AppendBatchAsync(IReadOnlyList<QueueEventRecord> records, CancellationToken ct = default)
        {
            ThrowIfDisposed();

            if (records == null)
                throw new ArgumentNullException(nameof(records));

            if (records.Count == 0)
                return;

            ct.ThrowIfCancellationRequested();

            var tenantId = ValidateAppendBatch(records);
            var request = new PendingAppendRequest(CloneRecords(records), EstimateBatchSize(records));
            GetOrCreateTenantWriter(tenantId).Enqueue(request);

            if (_ackMode == QueueEventJournalAckMode.Async)
                return;

            await request.Completion.Task.ConfigureAwait(false);
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

            ValidateTenantIdForPath(tenantId, nameof(tenantId));

            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset), "Offset cannot be negative.");

            if (maxRecords <= 0)
                throw new ArgumentOutOfRangeException(nameof(maxRecords), "MaxRecords must be greater than zero.");

            var state = LoadJournalState(tenantId);
            var path = GetJournalPath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return new QueueEventReadBatch(Array.Empty<QueueEventRecord>(), state.BaseOffset, true);

            var effectiveOffset = offset < state.BaseOffset ? state.BaseOffset : offset;
            var nextRelativeOffset = effectiveOffset - state.BaseOffset;
            await RepairCorruptTailIfNeededAsync(tenantId, state, ct).ConfigureAwait(false);

            using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
            {
                if (nextRelativeOffset > stream.Length)
                    nextRelativeOffset = stream.Length;

                var codec = GetCodec(tenantId);
                var result = await codec.ReadBatchAsync(stream, nextRelativeOffset, maxRecords, ct).ConfigureAwait(false);
                if (result.EncounteredCorruptTail)
                {
                    QueueJournalMetrics.RecordCorruptTailDetected(codec.Format, "read");
                    _logger.LogWarning(
                        CorruptTailReadEventId,
                        "Queue journal corrupt tail detected for tenant {TenantId} at offset {Offset} while reading {Format}.",
                        tenantId,
                        state.BaseOffset + result.NextOffset,
                        codec.Format);
                }

                return new QueueEventReadBatch(
                    result.Records,
                    state.BaseOffset + result.NextOffset,
                    result.ReachedEndOfFile);
            }
        }

        /// <inheritdoc/>
        public async Task<long> CompactAsync(string tenantId, long processedOffset, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            ValidateTenantIdForPath(tenantId, nameof(tenantId));

            if (processedOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(processedOffset), "Processed offset cannot be negative.");

            var appendLock = _appendLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                CloseTenantAppendStream(tenantId);

                var state = LoadJournalState(tenantId);
                await RepairCorruptTailCoreAsync(tenantId, state, appendLock, ct, lockAlreadyHeld: true).ConfigureAwait(false);
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

                ReplaceFileAtomically(tempPath, path);
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

            ValidateTenantIdForPath(tenantId, nameof(tenantId));

            ct.ThrowIfCancellationRequested();
            var state = LoadJournalState(tenantId);
            return Task.FromResult(state.TailOffset);
        }

        /// <inheritdoc/>
        public Task<long> GetBaseOffsetAsync(string tenantId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            ValidateTenantIdForPath(tenantId, nameof(tenantId));

            ct.ThrowIfCancellationRequested();
            var state = LoadJournalState(tenantId);
            return Task.FromResult(state.BaseOffset);
        }

        private TenantWriterState GetOrCreateTenantWriter(string tenantId)
        {
            return _tenantWriters.GetOrAdd(tenantId, CreateTenantWriter);
        }

        private TenantWriterState CreateTenantWriter(string tenantId)
        {
            var writer = new TenantWriterState(tenantId);
            writer.WorkerTask = Task.Run(() => RunTenantWriterAsync(writer));
            return writer;
        }

        private async Task RunTenantWriterAsync(TenantWriterState writer)
        {
            try
            {
                while (true)
                {
                    if (writer.ShutdownRequested && writer.PendingRequests.IsEmpty)
                        break;

                    var firstRequest = await WaitForNextRequestAsync(writer).ConfigureAwait(false);
                    if (firstRequest == null)
                        continue;

                    var batch = await CollectBatchAsync(writer, firstRequest).ConfigureAwait(false);
                    try
                    {
                        await WriteBatchAsync(writer, batch).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        CloseTenantAppendStream(writer);
                        FailRequests(batch, ex);
                        _logger.LogError(ex, "Queue journal append batch failed for tenant {TenantId}", writer.TenantId);
                    }
                }

                await FlushAppendStreamIfNeededAsync(writer).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (writer.ShutdownRequested)
            {
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Queue journal writer crashed for tenant {TenantId}", writer.TenantId);
                FailPendingRequests(writer, ex);
            }
            finally
            {
                CloseTenantAppendStream(writer);
            }
        }

        private async Task<PendingAppendRequest?> WaitForNextRequestAsync(TenantWriterState writer)
        {
            while (true)
            {
                if (TryDequeuePendingRequest(writer, out var request))
                    return request;

                if (writer.ShutdownRequested)
                    return null;

                if (_ackMode == QueueEventJournalAckMode.Balanced && writer.HasUnflushedData)
                {
                    var flushDelay = GetBalancedFlushDelay(writer);
                    if (flushDelay <= TimeSpan.Zero)
                    {
                        await FlushAppendStreamIfNeededAsync(writer).ConfigureAwait(false);
                        continue;
                    }

                    var waitTimeout = GetNextIdleWaitTimeout(flushDelay);
                    var signaled = await writer.Signal.WaitAsync(waitTimeout, writer.Cancellation.Token).ConfigureAwait(false);
                    if (!signaled)
                    {
                        if (flushDelay <= waitTimeout)
                            await FlushAppendStreamIfNeededAsync(writer).ConfigureAwait(false);
                        else
                            CloseTenantAppendStream(writer);

                        continue;
                    }

                    if (writer.PendingRequests.TryDequeue(out request))
                        return request;

                    continue;
                }

                if (_writerIdleTimeout == TimeSpan.Zero)
                {
                    CloseTenantAppendStream(writer);
                    await writer.Signal.WaitAsync(writer.Cancellation.Token).ConfigureAwait(false);
                    if (writer.PendingRequests.TryDequeue(out request))
                        return request;

                    continue;
                }

                var hasSignal = await writer.Signal.WaitAsync(_writerIdleTimeout, writer.Cancellation.Token).ConfigureAwait(false);
                if (!hasSignal)
                {
                    CloseTenantAppendStream(writer);
                    continue;
                }

                if (writer.PendingRequests.TryDequeue(out request))
                    return request;
            }
        }

        private async Task<List<PendingAppendRequest>> CollectBatchAsync(TenantWriterState writer, PendingAppendRequest firstRequest)
        {
            var batch = new List<PendingAppendRequest> { firstRequest };
            var totalRecords = firstRequest.RecordCount;
            var totalBytes = firstRequest.EstimatedBytes;
            var sawBacklog = !writer.PendingRequests.IsEmpty;

            if (_linger <= TimeSpan.Zero)
            {
                DrainQueuedRequests(writer, batch, ref totalRecords, ref totalBytes);
                return batch;
            }

            var deadlineUtc = DateTime.UtcNow + _linger;
            while (true)
            {
                if (DrainQueuedRequests(writer, batch, ref totalRecords, ref totalBytes))
                    sawBacklog = true;

                if (totalRecords >= _maxBatchRecords || totalBytes >= _maxBatchBytes || !sawBacklog)
                    break;

                var remaining = deadlineUtc - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero)
                    break;

                var delay = remaining < QueuePollInterval ? remaining : QueuePollInterval;
                await Task.Delay(delay, writer.Cancellation.Token).ConfigureAwait(false);
            }

            return batch;
        }

        private bool DrainQueuedRequests(
            TenantWriterState writer,
            List<PendingAppendRequest> batch,
            ref int totalRecords,
            ref int totalBytes)
        {
            var drainedAny = false;
            while (totalRecords < _maxBatchRecords
                && totalBytes < _maxBatchBytes
                && TryDequeuePendingRequest(writer, out var nextRequest))
            {
                batch.Add(nextRequest);
                totalRecords += nextRequest.RecordCount;
                totalBytes += nextRequest.EstimatedBytes;
                drainedAny = true;
            }

            return drainedAny;
        }

        private bool TryDequeuePendingRequest(TenantWriterState writer, out PendingAppendRequest request)
        {
            if (writer.PendingRequests.TryDequeue(out request))
            {
                writer.Signal.Wait(0);
                return true;
            }

            request = null!;
            return false;
        }

        private async Task WriteBatchAsync(TenantWriterState writer, IReadOnlyList<PendingAppendRequest> batch)
        {
            var batchStartedAt = Stopwatch.GetTimestamp();
            var appendLock = _appendLocks.GetOrAdd(writer.TenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(writer.Cancellation.Token).ConfigureAwait(false);
            try
            {
                var state = LoadJournalState(writer.TenantId);
                await RepairCorruptTailCoreAsync(writer.TenantId, state, appendLock, writer.Cancellation.Token, lockAlreadyHeld: true).ConfigureAwait(false);
                var serializedBatch = SerializeBatch(state, batch);
                if (serializedBatch.TotalBytes == 0)
                    return;

                var stream = EnsureAppendStream(writer);
                var shouldFlushBeforeAck = ShouldFlushBeforeAck(writer);
                if (serializedBatch.Records.Count == 1)
                {
                    var singleRecord = serializedBatch.Records[0].Bytes;
                    await stream.WriteAsync(singleRecord, 0, singleRecord.Length, writer.Cancellation.Token).ConfigureAwait(false);

                    state.TailOffset += singleRecord.Length;
                    MarkJournalStateDirty(writer.TenantId);

                    if (shouldFlushBeforeAck)
                        await FlushAppendStreamCoreAsync(writer, "before_ack").ConfigureAwait(false);
                    else
                        writer.HasUnflushedData = true;

                    QueueJournalOperationMetrics.RecordAppendBatch(
                        state.Format,
                        _ackMode,
                        batch.Count,
                        serializedBatch.Records.Count,
                        singleRecord.Length,
                        shouldFlushBeforeAck,
                        "single_record",
                        Stopwatch.GetTimestamp() - batchStartedAt);
                    CompleteRequests(batch);
                    return;
                }

                var writeBuffer = ArrayPool<byte>.Shared.Rent(serializedBatch.TotalBytes);
                try
                {
                    var writeOffset = 0;
                    foreach (var serializedRecord in serializedBatch.Records)
                    {
                        Buffer.BlockCopy(serializedRecord.Bytes, 0, writeBuffer, writeOffset, serializedRecord.Bytes.Length);
                        writeOffset += serializedRecord.Bytes.Length;
                    }

                    await stream.WriteAsync(writeBuffer, 0, serializedBatch.TotalBytes, writer.Cancellation.Token).ConfigureAwait(false);

                    state.TailOffset += serializedBatch.TotalBytes;
                    MarkJournalStateDirty(writer.TenantId);

                    if (shouldFlushBeforeAck)
                        await FlushAppendStreamCoreAsync(writer, "before_ack").ConfigureAwait(false);
                    else
                        writer.HasUnflushedData = true;

                    QueueJournalOperationMetrics.RecordAppendBatch(
                        state.Format,
                        _ackMode,
                        batch.Count,
                        serializedBatch.Records.Count,
                        serializedBatch.TotalBytes,
                        shouldFlushBeforeAck,
                        "buffered_batch",
                        Stopwatch.GetTimestamp() - batchStartedAt);
                    CompleteRequests(batch);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(writeBuffer);
                }
            }
            finally
            {
                appendLock.Release();
            }
        }

        private SerializedBatch SerializeBatch(JournalState state, IReadOnlyList<PendingAppendRequest> batch)
        {
            var totalRecordCount = 0;
            foreach (var request in batch)
                totalRecordCount += request.RecordCount;

            var serializedRecords = new List<SerializedRecord>(totalRecordCount);
            var nextSequence = state.LastSequenceNumber;
            var codec = GetCodec(state.Format);
            var totalBytes = 0;

            foreach (var request in batch)
            {
                foreach (var record in request.Records)
                {
                    nextSequence++;
                    var bytes = codec.SerializeRecord(record, nextSequence);
                    serializedRecords.Add(new SerializedRecord(bytes));
                    totalBytes += bytes.Length;
                }
            }

            state.LastSequenceNumber = nextSequence;
            return new SerializedBatch(serializedRecords, totalBytes);
        }

        private Stream EnsureAppendStream(TenantWriterState writer)
        {
            if (writer.AppendStream != null)
                return writer.AppendStream;

            var tenantDirectory = GetTenantDirectory(writer.TenantId);
            if (!_fileSystem.Directory.Exists(tenantDirectory))
                _fileSystem.Directory.CreateDirectory(tenantDirectory);

            writer.AppendStream = _fileSystem.File.Open(
                GetJournalPath(writer.TenantId),
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read | FileShare.Delete);
            writer.LastFlushUtc = DateTime.UtcNow;
            return writer.AppendStream;
        }

        private bool ShouldFlushBeforeAck(TenantWriterState writer)
        {
            if (_ackMode == QueueEventJournalAckMode.Durable)
                return true;

            if (_ackMode != QueueEventJournalAckMode.Balanced)
                return false;

            if (writer.PendingRequests.IsEmpty)
                return true;

            return GetBalancedFlushDelay(writer) <= TimeSpan.Zero;
        }

        private TimeSpan GetBalancedFlushDelay(TenantWriterState writer)
        {
            if (_balancedFlushWindow <= TimeSpan.Zero)
                return TimeSpan.Zero;

            var elapsed = DateTime.UtcNow - writer.LastFlushUtc;
            return elapsed >= _balancedFlushWindow
                ? TimeSpan.Zero
                : _balancedFlushWindow - elapsed;
        }

        private TimeSpan GetNextIdleWaitTimeout(TimeSpan flushDelay)
        {
            if (_writerIdleTimeout == TimeSpan.Zero)
                return flushDelay;

            return flushDelay <= _writerIdleTimeout ? flushDelay : _writerIdleTimeout;
        }

        private async Task FlushAppendStreamIfNeededAsync(TenantWriterState writer)
        {
            if (!writer.HasUnflushedData)
                return;

            var appendLock = _appendLocks.GetOrAdd(writer.TenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(writer.Cancellation.Token).ConfigureAwait(false);
            try
            {
                await FlushAppendStreamCoreAsync(writer, "deferred").ConfigureAwait(false);
            }
            finally
            {
                appendLock.Release();
            }
        }

        private async Task FlushAppendStreamCoreAsync(TenantWriterState writer, string reason)
        {
            var format = LoadJournalState(writer.TenantId).Format;
            var flushStartedAt = Stopwatch.GetTimestamp();
            if (writer.AppendStream != null)
                await DurableFileWrite.FlushToDiskAsync(writer.AppendStream, writer.Cancellation.Token).ConfigureAwait(false);

            writer.HasUnflushedData = false;
            writer.LastFlushUtc = DateTime.UtcNow;
            QueueJournalOperationMetrics.RecordFlush(
                format,
                _ackMode,
                reason,
                Stopwatch.GetTimestamp() - flushStartedAt);
        }

        private void CompleteRequests(IReadOnlyList<PendingAppendRequest> batch)
        {
            foreach (var request in batch)
                request.Completion.TrySetResult(null);
        }

        private void FailRequests(IReadOnlyList<PendingAppendRequest> batch, Exception exception)
        {
            foreach (var request in batch)
                request.Completion.TrySetException(exception);
        }

        private void FailPendingRequests(TenantWriterState writer, Exception exception)
        {
            while (writer.PendingRequests.TryDequeue(out var request))
                request.Completion.TrySetException(exception);
        }

        private string ValidateAppendBatch(IReadOnlyList<QueueEventRecord> records)
        {
            var tenantId = records[0]?.TenantId;
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(records));

            ValidateTenantIdForPath(tenantId, nameof(records));

            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i] ?? throw new ArgumentException("Queue event record cannot be null.", nameof(records));
                if (string.IsNullOrWhiteSpace(record.TenantId))
                    throw new ArgumentException("TenantId cannot be empty", nameof(records));

                ValidateTenantIdForPath(record.TenantId, nameof(records));

                if (string.IsNullOrWhiteSpace(record.FileKey))
                    throw new ArgumentException("FileKey cannot be empty", nameof(records));

                if (!string.Equals(record.TenantId, tenantId, StringComparison.Ordinal))
                    throw new ArgumentException("All queue event records in a batch must belong to the same tenant.", nameof(records));
            }

            return tenantId!;
        }

        private static void ValidateTenantIdForPath(string? tenantId, string paramName)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", paramName);

            var validatedTenantId = tenantId!;
            if (validatedTenantId.IndexOf('/') >= 0 || validatedTenantId.IndexOf('\\') >= 0 || validatedTenantId.Contains(".."))
                throw new ArgumentException($"TenantId contains invalid path characters: '{validatedTenantId}'", paramName);
        }

        private IReadOnlyList<QueueEventRecord> CloneRecords(IReadOnlyList<QueueEventRecord> records)
        {
            var clones = new QueueEventRecord[records.Count];
            for (var i = 0; i < records.Count; i++)
                clones[i] = CloneRecord(records[i]);

            return clones;
        }

        private static QueueEventRecord CloneRecord(QueueEventRecord record)
        {
            return new QueueEventRecord
            {
                SchemaVersion = record.SchemaVersion,
                EventId = record.EventId,
                TenantId = record.TenantId,
                FileKey = record.FileKey,
                EventType = record.EventType,
                OccurredAtUtc = record.OccurredAtUtc,
                SequenceNumber = record.SequenceNumber,
                PayloadCrc32 = record.PayloadCrc32,
                VolumeId = record.VolumeId,
                PhysicalPath = record.PhysicalPath,
                DirectoryPath = record.DirectoryPath,
                FileSize = record.FileSize,
                Status = record.Status,
                ProcessingStartTimeUtc = record.ProcessingStartTimeUtc,
                RetryCount = record.RetryCount,
                AvailableForProcessingAtUtc = record.AvailableForProcessingAtUtc,
                ErrorMessage = record.ErrorMessage,
                OriginalFileName = record.OriginalFileName,
                FileExtension = record.FileExtension,
            };
        }

        private static int EstimateBatchSize(IReadOnlyList<QueueEventRecord> records)
        {
            var total = 0;
            for (var i = 0; i < records.Count; i++)
                total += EstimateRecordSize(records[i]);

            return total;
        }

        private static int EstimateRecordSize(QueueEventRecord record)
        {
            var total = 256;
            total += record.EventId?.Length ?? 0;
            total += record.TenantId?.Length ?? 0;
            total += record.FileKey?.Length ?? 0;
            total += record.VolumeId?.Length ?? 0;
            total += record.PhysicalPath?.Length ?? 0;
            total += record.DirectoryPath?.Length ?? 0;
            total += record.ErrorMessage?.Length ?? 0;
            total += record.OriginalFileName?.Length ?? 0;
            total += record.FileExtension?.Length ?? 0;
            return total;
        }

        private void CloseTenantAppendStream(string tenantId)
        {
            if (_tenantWriters.TryGetValue(tenantId, out var writer))
                CloseTenantAppendStream(writer);
        }

        private void CloseTenantAppendStream(TenantWriterState writer)
        {
            var stream = writer.AppendStream;
            writer.AppendStream = null;
            if (stream == null)
                return;

            try
            {
                stream.Dispose();
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(FileQueueEventJournal));
        }

        private string GetTenantDirectory(string tenantId)
        {
            ValidateTenantIdForPath(tenantId, nameof(tenantId));
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

        private IQueueEventJournalCodec GetCodec(string tenantId)
        {
            return GetCodec(ResolveJournalFormat(tenantId));
        }

        private IQueueEventJournalCodec GetCodec(JournalFormat format)
        {
            return format == JournalFormat.BinaryV1 ? _binaryCodec : _jsonCodec;
        }

        private JournalFormat ResolveJournalFormat(string tenantId)
        {
            var path = GetJournalPath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return _defaultFormat;

            try
            {
                var fileInfo = _fileSystem.FileInfo.New(path);
                if (fileInfo.Length == 0)
                    return _defaultFormat;

                using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                {
                    var prefix = new byte[4];
                    var bytesRead = stream.Read(prefix, 0, prefix.Length);
                    if (BinaryQueueEventJournalCodec.MatchesMagic(prefix, bytesRead))
                        return JournalFormat.BinaryV1;

                    if (bytesRead > 0 && prefix[0] == (byte)'{')
                        return JournalFormat.JsonLines;
                }
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                _logger.LogWarning(ex, "Failed to detect queue journal format for tenant {TenantId}. Falling back to configured default.", tenantId);
            }

            return _defaultFormat;
        }

        private async Task RepairCorruptTailIfNeededAsync(string tenantId, JournalState state, CancellationToken ct)
        {
            var appendLock = _appendLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await RepairCorruptTailCoreAsync(tenantId, state, appendLock, ct, lockAlreadyHeld: false).ConfigureAwait(false);
        }

        private async Task RepairCorruptTailCoreAsync(
            string tenantId,
            JournalState state,
            SemaphoreSlim appendLock,
            CancellationToken ct,
            bool lockAlreadyHeld)
        {
            var path = GetJournalPath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return;

            var expectedLength = Math.Max(0, state.TailOffset - state.BaseOffset);
            var fileLength = _fileSystem.FileInfo.New(path).Length;
            if (fileLength <= expectedLength)
                return;

            if (!lockAlreadyHeld)
                await appendLock.WaitAsync(ct).ConfigureAwait(false);

            try
            {
                fileLength = _fileSystem.FileInfo.New(path).Length;
                if (fileLength <= expectedLength)
                    return;

                CloseTenantAppendStream(tenantId);
                using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Write, FileShare.Read))
                {
                    stream.SetLength(expectedLength);
                    await DurableFileWrite.FlushToDiskAsync(stream, ct).ConfigureAwait(false);
                }

                state.CorruptTailDetected = true;
                state.LastCorruptTailDetectedAtUtc = DateTime.UtcNow;
                state.LastCorruptTailOffset = state.BaseOffset + expectedLength;
                state.AutoRepairCount++;
                QueueJournalMetrics.RecordCorruptTailAutoRepaired(state.Format);

                _logger.LogWarning(
                    CorruptTailTruncatedEventId,
                    "Truncated corrupt queue journal tail for tenant {TenantId} from {OriginalLength} bytes to {RecoveredLength} bytes.",
                    tenantId,
                    fileLength,
                    expectedLength);

                SaveJournalState(tenantId, state);
            }
            finally
            {
                if (!lockAlreadyHeld)
                    appendLock.Release();
            }
        }

        private QueueEventJournalCodecScanResult ScanJournal(string tenantId, JournalFormat format)
        {
            var path = GetJournalPath(tenantId);
            if (!_fileSystem.File.Exists(path))
                return new QueueEventJournalCodecScanResult(0, 0, false);

            using (var stream = _fileSystem.File.Open(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                return GetCodec(format).Scan(stream);
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
                    // Allow readers to coexist with atomic state-file replacement on Windows.
                    using (var stream = _fileSystem.File.Open(
                        path,
                        FileMode.Open,
                        FileAccess.Read,
                        FileShare.ReadWrite | FileShare.Delete))
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
            var tempPath = path + ".tmp." + Guid.NewGuid().ToString("N");
            try
            {
                using (var stream = _fileSystem.File.Open(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    JsonSerializer.Serialize(stream, state, JsonOptions);
                    DurableFileWrite.FlushToDisk(stream);
                }

                ReplaceFileAtomically(tempPath, path);
                _stateCache[tenantId] = state;
                _dirtyStateTenants.TryRemove(tenantId, out _);
            }
            finally
            {
                if (_fileSystem.File.Exists(tempPath))
                {
                    try
                    {
                        _fileSystem.File.Delete(tempPath);
                    }
                    catch (IOException)
                    {
                    }
                    catch (UnauthorizedAccessException)
                    {
                    }
                }
            }
        }

        private void MarkJournalStateDirty(string tenantId)
        {
            if (_stateFlushDebounce == TimeSpan.Zero)
            {
                SaveJournalState(tenantId, LoadJournalState(tenantId));
                return;
            }

            _dirtyStateTenants[tenantId] = 0;
            if (_disposed || _stateFlushTimer == null)
                return;

            try
            {
                _stateFlushTimer.Change(_stateFlushDebounce, Timeout.InfiniteTimeSpan);
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void OnStateFlushTimer(object state)
        {
            _ = FlushDirtyJournalStatesAsync();
        }

        private async Task FlushDirtyJournalStatesAsync()
        {
            if (_disposed)
                return;

            if (Interlocked.CompareExchange(ref _stateFlushInProgress, 1, 0) != 0)
                return;

            try
            {
                var dirtyTenants = _dirtyStateTenants.Keys.ToArray();
                foreach (var tenantId in dirtyTenants)
                {
                    if (!_dirtyStateTenants.ContainsKey(tenantId))
                        continue;

                    var appendLock = _appendLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
                    await appendLock.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        if (_dirtyStateTenants.ContainsKey(tenantId))
                            SaveJournalState(tenantId, LoadJournalState(tenantId));
                    }
                    finally
                    {
                        appendLock.Release();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to flush debounced queue journal state file");
            }
            finally
            {
                Interlocked.Exchange(ref _stateFlushInProgress, 0);
                if (!_disposed && !_dirtyStateTenants.IsEmpty && _stateFlushTimer != null)
                {
                    try
                    {
                        _stateFlushTimer.Change(_stateFlushDebounce, Timeout.InfiniteTimeSpan);
                    }
                    catch (ObjectDisposedException)
                    {
                    }
                }
            }
        }

        private void ReplaceFileAtomically(string tempPath, string destinationPath)
        {
            const int maxAttempts = 5;
            for (var attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    if (_fileSystem.File.Exists(destinationPath))
                    {
                        _fileSystem.File.Replace(tempPath, destinationPath, null);
                    }
                    else
                    {
                        _fileSystem.File.Move(tempPath, destinationPath);
                    }

                    return;
                }
                catch (Exception ex) when (IsTransientReplaceFailure(ex) && attempt < maxAttempts - 1)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(15 * (attempt + 1)));
                }
            }

            if (_fileSystem.File.Exists(destinationPath))
            {
                _fileSystem.File.Replace(tempPath, destinationPath, null);
                return;
            }

            _fileSystem.File.Move(tempPath, destinationPath);
        }

        private static bool IsTransientReplaceFailure(Exception exception)
        {
            return exception is IOException || exception is UnauthorizedAccessException;
        }

        private void NormalizeJournalState(string tenantId, JournalState state)
        {
            if (state.BaseOffset < 0)
                state.BaseOffset = 0;

            if (state.LastSequenceNumber < 0)
                state.LastSequenceNumber = 0;

            state.Format = ResolveJournalFormat(tenantId);
            var path = GetJournalPath(tenantId);
            var fileLength = _fileSystem.File.Exists(path)
                ? _fileSystem.FileInfo.New(path).Length
                : 0;

            if (state.TailOffset < state.BaseOffset)
                state.TailOffset = state.BaseOffset;

            if (fileLength > 0)
            {
                try
                {
                    var scan = ScanJournal(tenantId, state.Format);
                    state.TailOffset = state.BaseOffset + scan.LastValidOffset;
                    state.LastSequenceNumber = Math.Max(state.LastSequenceNumber, scan.LastSequenceNumber);

                    if (scan.EncounteredCorruptTail)
                    {
                        state.CorruptTailDetected = true;
                        state.LastCorruptTailDetectedAtUtc = DateTime.UtcNow;
                        state.LastCorruptTailOffset = state.TailOffset;
                        QueueJournalMetrics.RecordCorruptTailDetected(state.Format, "state_scan");
                        _logger.LogWarning(
                            CorruptTailStateScanEventId,
                            "Queue journal corrupt tail detected for tenant {TenantId} while loading state. Using recovered tail offset {TailOffset} for format {Format}.",
                            tenantId,
                            state.TailOffset,
                            state.Format);
                    }
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException || ex is InvalidDataException)
                {
                    _logger.LogWarning(ex, "Failed to scan queue journal state for tenant {TenantId}", tenantId);
                    state.TailOffset = Math.Max(state.TailOffset, state.BaseOffset + fileLength);
                }
            }
            else
            {
                state.TailOffset = state.BaseOffset;
            }
        }

        private sealed class PendingAppendRequest
        {
            public PendingAppendRequest(IReadOnlyList<QueueEventRecord> records, int estimatedBytes)
            {
                Records = records ?? throw new ArgumentNullException(nameof(records));
                EstimatedBytes = estimatedBytes;
                Completion = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            public IReadOnlyList<QueueEventRecord> Records { get; }

            public int RecordCount => Records.Count;

            public int EstimatedBytes { get; }

            public TaskCompletionSource<object?> Completion { get; }
        }

        private sealed class TenantWriterState
        {
            public TenantWriterState(string tenantId)
            {
                TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
                PendingRequests = new ConcurrentQueue<PendingAppendRequest>();
                Signal = new SemaphoreSlim(0);
                Cancellation = new CancellationTokenSource();
                LastFlushUtc = DateTime.UtcNow;
            }

            public string TenantId { get; }

            public ConcurrentQueue<PendingAppendRequest> PendingRequests { get; }

            public SemaphoreSlim Signal { get; }

            public CancellationTokenSource Cancellation { get; }

            public Task WorkerTask { get; set; } = Task.CompletedTask;

            public Stream? AppendStream { get; set; }

            public bool HasUnflushedData { get; set; }

            public DateTime LastFlushUtc { get; set; }

            public bool ShutdownRequested { get; private set; }

            public void Enqueue(PendingAppendRequest request)
            {
                PendingRequests.Enqueue(request);
                Signal.Release();
            }

            public void RequestShutdown()
            {
                ShutdownRequested = true;
                Signal.Release();
            }
        }

        private sealed class SerializedRecord
        {
            public SerializedRecord(byte[] bytes)
            {
                Bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
            }

            public byte[] Bytes { get; }
        }

        private sealed class SerializedBatch
        {
            public SerializedBatch(IReadOnlyList<SerializedRecord> records, int totalBytes)
            {
                Records = records ?? throw new ArgumentNullException(nameof(records));
                TotalBytes = totalBytes;
            }

            public IReadOnlyList<SerializedRecord> Records { get; }

            public int TotalBytes { get; }
        }

        private sealed class JournalState
        {
            public long BaseOffset { get; set; }

            public long TailOffset { get; set; }

            public long LastSequenceNumber { get; set; }

            [JsonConverter(typeof(JsonStringEnumConverter))]
            public JournalFormat Format { get; set; } = JournalFormat.JsonLines;

            public bool CorruptTailDetected { get; set; }

            public DateTime? LastCorruptTailDetectedAtUtc { get; set; }

            public long? LastCorruptTailOffset { get; set; }

            public int AutoRepairCount { get; set; }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_stateFlushTimer != null)
            {
                try
                {
                    _stateFlushTimer.Change(Timeout.Infinite, Timeout.Infinite);
                }
                catch (ObjectDisposedException)
                {
                }

                _stateFlushTimer.Dispose();
            }

            foreach (var tenantWriter in _tenantWriters.Values)
                tenantWriter.RequestShutdown();

            foreach (var tenantWriter in _tenantWriters.Values)
            {
                try
                {
                    tenantWriter.WorkerTask.GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed while draining queue journal writer for tenant {TenantId}", tenantWriter.TenantId);
                }
            }

            if (!_dirtyStateTenants.IsEmpty)
            {
                try
                {
                    foreach (var tenantId in _dirtyStateTenants.Keys.ToArray())
                        SaveJournalState(tenantId, LoadJournalState(tenantId));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to flush queue journal state during disposal");
                }
            }
        }
    }
}
