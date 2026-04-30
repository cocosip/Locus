using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Linq;
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
    public class FileQueueEventJournal : IQueueEventJournal, IQueueEventJournalWritePathDiagnostics, IDisposable
    {
        private static readonly EventId CorruptTailReadEventId = new EventId(4101, nameof(CorruptTailReadEventId));
        private static readonly EventId CorruptTailTruncatedEventId = new EventId(4102, nameof(CorruptTailTruncatedEventId));
        private static readonly EventId CorruptTailStateScanEventId = new EventId(4103, nameof(CorruptTailStateScanEventId));
        private static readonly EventId MisalignedReadOffsetRecoveredEventId = new EventId(4104, nameof(MisalignedReadOffsetRecoveredEventId));
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };
        private static readonly TimeSpan DefaultStateFlushDebounce = TimeSpan.FromSeconds(1);
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
        private long _observedAppendBatchCount;
        private long _observedSingleRecordAppendBatchCount;
        private long _observedMultiRecordAppendBatchCount;
        private long _observedAppendedRecordCount;
        private long _observedAppendedBytes;
        private long _observedAppendTicks;
        private long _observedFlushCount;
        private long _observedFlushTicks;
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
            ThrowIfDisposed();

            if (record == null)
                throw new ArgumentNullException(nameof(record));

            ct.ThrowIfCancellationRequested();

            var tenantId = ValidateAppendRecord(record, nameof(record));
            if (_ackMode == QueueEventJournalAckMode.Durable)
            {
                var directWriter = GetOrCreateTenantWriter(tenantId, ensureWorkerStarted: false);
                return AppendDirectAsync(directWriter, CloneRecord(record), ct);
            }

            var request = new PendingAppendRequest(
                CloneRecord(record),
                EstimateRecordSize(record),
                createCompletion: _ackMode != QueueEventJournalAckMode.Async);
            GetOrCreateTenantWriter(tenantId, ensureWorkerStarted: true).Enqueue(request);

            if (_ackMode == QueueEventJournalAckMode.Async)
                return Task.CompletedTask;

            return request.Completion!.Task;
        }

        /// <inheritdoc/>
        public async Task AppendBatchAsync(IReadOnlyList<QueueEventRecord> records, CancellationToken ct = default)
        {
            ThrowIfDisposed();

            if (records == null)
                throw new ArgumentNullException(nameof(records));

            if (records.Count == 0)
                return;

            if (records.Count == 1 && records[0] != null)
            {
                await AppendAsync(records[0], ct).ConfigureAwait(false);
                return;
            }

            ct.ThrowIfCancellationRequested();

            var tenantId = ValidateAppendBatch(records);
            if (_ackMode == QueueEventJournalAckMode.Durable)
            {
                var directWriter = GetOrCreateTenantWriter(tenantId, ensureWorkerStarted: false);
                await AppendBatchDirectAsync(directWriter, CloneRecords(records), ct).ConfigureAwait(false);
                return;
            }

            var request = new PendingAppendRequest(
                CloneRecords(records),
                EstimateBatchSize(records),
                createCompletion: _ackMode != QueueEventJournalAckMode.Async);
            GetOrCreateTenantWriter(tenantId, ensureWorkerStarted: true).Enqueue(request);

            if (_ackMode == QueueEventJournalAckMode.Async)
                return;

            await request.Completion!.Task.ConfigureAwait(false);
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
                    QueueJournalMetrics.RecordCorruptTailDetected();

                    var normalizedRelativeOffset = codec.NormalizeReadOffset(stream, nextRelativeOffset);
                    if (normalizedRelativeOffset < nextRelativeOffset)
                    {
                        var recoveredResult = await codec.ReadBatchAsync(stream, normalizedRelativeOffset, maxRecords, ct).ConfigureAwait(false);
                        if (!recoveredResult.EncounteredCorruptTail)
                        {
                            _logger.LogDebug(
                                MisalignedReadOffsetRecoveredEventId,
                                "Recovered queue journal read offset for tenant {TenantId} from {RequestedOffset} to {RecoveredOffset} while reading {Format}.",
                                tenantId,
                                state.BaseOffset + nextRelativeOffset,
                                state.BaseOffset + normalizedRelativeOffset,
                                codec.Format);

                            return new QueueEventReadBatch(
                                recoveredResult.Records,
                                state.BaseOffset + recoveredResult.NextOffset,
                                recoveredResult.ReachedEndOfFile);
                        }
                    }

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

        /// <inheritdoc/>
        public QueueJournalWritePathStatistics GetWritePathStatisticsSnapshot()
        {
            return new QueueJournalWritePathStatistics
            {
                AppendBatchCount = Interlocked.Read(ref _observedAppendBatchCount),
                SingleRecordAppendBatchCount = Interlocked.Read(ref _observedSingleRecordAppendBatchCount),
                MultiRecordAppendBatchCount = Interlocked.Read(ref _observedMultiRecordAppendBatchCount),
                AppendedRecordCount = Interlocked.Read(ref _observedAppendedRecordCount),
                AppendedBytes = Interlocked.Read(ref _observedAppendedBytes),
                AppendTicks = Interlocked.Read(ref _observedAppendTicks),
                FlushCount = Interlocked.Read(ref _observedFlushCount),
                FlushTicks = Interlocked.Read(ref _observedFlushTicks),
            };
        }

        private TenantWriterState GetOrCreateTenantWriter(string tenantId, bool ensureWorkerStarted)
        {
            var writer = _tenantWriters.GetOrAdd(tenantId, id => new TenantWriterState(id));
            if (ensureWorkerStarted)
                EnsureTenantWriterStarted(writer);

            return writer;
        }

        private void EnsureTenantWriterStarted(TenantWriterState writer)
        {
            if (Interlocked.CompareExchange(ref writer.WorkerStarted, 1, 0) != 0)
                return;

            writer.WorkerTask = Task.Run(() => RunTenantWriterAsync(writer));
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

                var signaled = await writer.Signal.WaitAsync(remaining, writer.Cancellation.Token).ConfigureAwait(false);
                if (!signaled)
                    break;

                if (writer.PendingRequests.TryDequeue(out var nextRequest))
                {
                    batch.Add(nextRequest);
                    totalRecords += nextRequest.RecordCount;
                    totalBytes += nextRequest.EstimatedBytes;
                    sawBacklog = true;
                }
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
                var stream = EnsureAppendStream(writer);
                var shouldFlushBeforeAck = ShouldFlushBeforeAck(writer);
                if (batch.Count == 1 && batch[0].TryGetSingleRecord(out var singleAppendRecord))
                {
                    var singleRecord = SerializeSingleRecord(state, singleAppendRecord);
                    await stream.WriteAsync(singleRecord, 0, singleRecord.Length, writer.Cancellation.Token).ConfigureAwait(false);

                    state.TailOffset += singleRecord.Length;
                    MarkJournalStateDirty(writer.TenantId);
                    var appendDurationTicks = Stopwatch.GetTimestamp() - batchStartedAt;

                    if (shouldFlushBeforeAck)
                        await FlushAppendStreamCoreAsync(writer).ConfigureAwait(false);
                    else
                        writer.HasUnflushedData = true;

                    var durationTicks = Stopwatch.GetTimestamp() - batchStartedAt;
                    QueueJournalOperationMetrics.RecordAppendBatch(
                        singleRecord.Length,
                        durationTicks);
                    RecordObservedAppendBatch(recordCount: 1, totalBytes: singleRecord.Length, durationTicks: appendDurationTicks);
                    CompleteRequests(batch);
                    return;
                }

                using (var serializedBatch = SerializeBatch(state, batch, out var batchRecords))
                {
                    if (serializedBatch.Length == 0)
                        return;

                    await stream.WriteAsync(serializedBatch.Buffer, 0, serializedBatch.Length, writer.Cancellation.Token).ConfigureAwait(false);

                    state.TailOffset += serializedBatch.Length;
                    MarkJournalStateDirty(writer.TenantId);
                    var appendDurationTicks = Stopwatch.GetTimestamp() - batchStartedAt;

                    if (shouldFlushBeforeAck)
                        await FlushAppendStreamCoreAsync(writer).ConfigureAwait(false);
                    else
                        writer.HasUnflushedData = true;

                    var durationTicks = Stopwatch.GetTimestamp() - batchStartedAt;
                    QueueJournalOperationMetrics.RecordAppendBatch(
                        serializedBatch.Length,
                        durationTicks);
                    RecordObservedAppendBatch(
                        recordCount: batchRecords.Length,
                        totalBytes: serializedBatch.Length,
                        durationTicks: appendDurationTicks);
                    CompleteRequests(batch);
                }
            }
            finally
            {
                appendLock.Release();
            }
        }

        private async Task AppendDirectAsync(TenantWriterState writer, QueueEventRecord record, CancellationToken ct)
        {
            var appendStartedAt = Stopwatch.GetTimestamp();
            var appendLock = _appendLocks.GetOrAdd(writer.TenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var state = LoadJournalState(writer.TenantId);
                await RepairCorruptTailCoreAsync(writer.TenantId, state, appendLock, ct, lockAlreadyHeld: true).ConfigureAwait(false);
                var stream = EnsureAppendStream(writer);
                var serializedRecord = SerializeSingleRecord(state, record);
                ct.ThrowIfCancellationRequested();
                stream.Write(serializedRecord, 0, serializedRecord.Length);

                state.TailOffset += serializedRecord.Length;
                MarkJournalStateDirty(writer.TenantId);
                var appendDurationTicks = Stopwatch.GetTimestamp() - appendStartedAt;
                FlushAppendStreamCore(writer);

                var durationTicks = Stopwatch.GetTimestamp() - appendStartedAt;
                QueueJournalOperationMetrics.RecordAppendBatch(serializedRecord.Length, durationTicks);
                RecordObservedAppendBatch(recordCount: 1, totalBytes: serializedRecord.Length, durationTicks: appendDurationTicks);
            }
            finally
            {
                appendLock.Release();
            }
        }

        private async Task AppendBatchDirectAsync(TenantWriterState writer, IReadOnlyList<QueueEventRecord> records, CancellationToken ct)
        {
            var appendStartedAt = Stopwatch.GetTimestamp();
            var appendLock = _appendLocks.GetOrAdd(writer.TenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var state = LoadJournalState(writer.TenantId);
                await RepairCorruptTailCoreAsync(writer.TenantId, state, appendLock, ct, lockAlreadyHeld: true).ConfigureAwait(false);
                var stream = EnsureAppendStream(writer);
                if (records.Count == 1)
                {
                    var serializedRecord = SerializeSingleRecord(state, records[0]);
                    ct.ThrowIfCancellationRequested();
                    stream.Write(serializedRecord, 0, serializedRecord.Length);

                    state.TailOffset += serializedRecord.Length;
                    MarkJournalStateDirty(writer.TenantId);
                    var appendDurationTicks = Stopwatch.GetTimestamp() - appendStartedAt;
                    FlushAppendStreamCore(writer);

                    var durationTicks = Stopwatch.GetTimestamp() - appendStartedAt;
                    QueueJournalOperationMetrics.RecordAppendBatch(serializedRecord.Length, durationTicks);
                    RecordObservedAppendBatch(recordCount: 1, totalBytes: serializedRecord.Length, durationTicks: appendDurationTicks);
                    return;
                }

                using (var serializedBatch = SerializeBatch(state, records))
                {
                    if (serializedBatch.Length == 0)
                        return;

                    ct.ThrowIfCancellationRequested();
                    stream.Write(serializedBatch.Buffer, 0, serializedBatch.Length);

                    state.TailOffset += serializedBatch.Length;
                    MarkJournalStateDirty(writer.TenantId);
                    var appendDurationTicks = Stopwatch.GetTimestamp() - appendStartedAt;
                    FlushAppendStreamCore(writer);

                    var durationTicks = Stopwatch.GetTimestamp() - appendStartedAt;
                    QueueJournalOperationMetrics.RecordAppendBatch(serializedBatch.Length, durationTicks);
                    RecordObservedAppendBatch(recordCount: records.Count, totalBytes: serializedBatch.Length, durationTicks: appendDurationTicks);
                }
            }
            finally
            {
                appendLock.Release();
            }
        }

        private byte[] SerializeSingleRecord(JournalState state, QueueEventRecord record)
        {
            var nextSequence = state.LastSequenceNumber + 1;
            var bytes = GetCodec(state.Format).SerializeRecord(record, nextSequence);
            state.LastSequenceNumber = nextSequence;
            return bytes;
        }

        private QueueEventJournalBatchBuffer SerializeBatch(JournalState state, IReadOnlyList<QueueEventRecord> records)
        {
            var serializedBatch = GetCodec(state.Format).SerializeBatch(records, state.LastSequenceNumber);
            state.LastSequenceNumber += records.Count;
            return serializedBatch;
        }

        private QueueEventJournalBatchBuffer SerializeBatch(JournalState state, IReadOnlyList<PendingAppendRequest> batch, out QueueEventRecord[] batchRecords)
        {
            var totalRecordCount = 0;
            foreach (var request in batch)
                totalRecordCount += request.RecordCount;

            batchRecords = new QueueEventRecord[totalRecordCount];
            var recordIndex = 0;

            foreach (var request in batch)
            {
                if (request.TryGetSingleRecord(out var singleRecord))
                {
                    batchRecords[recordIndex++] = singleRecord;
                    continue;
                }

                foreach (var record in request.Records)
                    batchRecords[recordIndex++] = record;
            }

            return SerializeBatch(state, batchRecords);
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
                await FlushAppendStreamCoreAsync(writer).ConfigureAwait(false);
            }
            finally
            {
                appendLock.Release();
            }
        }

        private async Task FlushAppendStreamCoreAsync(TenantWriterState writer)
        {
            var flushStartedAt = Stopwatch.GetTimestamp();
            if (writer.AppendStream != null)
                await DurableFileWrite.FlushToDiskAsync(writer.AppendStream, writer.Cancellation.Token).ConfigureAwait(false);

            CompleteAppendFlush(writer, flushStartedAt);
        }

        private void FlushAppendStreamCore(TenantWriterState writer)
        {
            var flushStartedAt = Stopwatch.GetTimestamp();
            if (writer.AppendStream != null)
                DurableFileWrite.FlushToDisk(writer.AppendStream);

            CompleteAppendFlush(writer, flushStartedAt);
        }

        private void CompleteAppendFlush(TenantWriterState writer, long flushStartedAt)
        {
            writer.HasUnflushedData = false;
            writer.LastFlushUtc = DateTime.UtcNow;
            var durationTicks = Stopwatch.GetTimestamp() - flushStartedAt;
            QueueJournalOperationMetrics.RecordFlush(durationTicks);
            Interlocked.Increment(ref _observedFlushCount);
            Interlocked.Add(ref _observedFlushTicks, durationTicks);
        }

        private void RecordObservedAppendBatch(int recordCount, int totalBytes, long durationTicks)
        {
            Interlocked.Increment(ref _observedAppendBatchCount);
            if (recordCount == 1)
                Interlocked.Increment(ref _observedSingleRecordAppendBatchCount);
            else
                Interlocked.Increment(ref _observedMultiRecordAppendBatchCount);

            Interlocked.Add(ref _observedAppendedRecordCount, recordCount);
            Interlocked.Add(ref _observedAppendedBytes, totalBytes);
            Interlocked.Add(ref _observedAppendTicks, durationTicks);
        }

        private void CompleteRequests(IReadOnlyList<PendingAppendRequest> batch)
        {
            foreach (var request in batch)
                request.TrySetCompleted();
        }

        private void FailRequests(IReadOnlyList<PendingAppendRequest> batch, Exception exception)
        {
            foreach (var request in batch)
                request.TrySetException(exception);
        }

        private void FailPendingRequests(TenantWriterState writer, Exception exception)
        {
            while (writer.PendingRequests.TryDequeue(out var request))
                request.TrySetException(exception);
        }

        private string ValidateAppendBatch(IReadOnlyList<QueueEventRecord> records)
        {
            var tenantId = ValidateAppendRecord(records[0], nameof(records));

            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i];
                ValidateAppendRecord(record, nameof(records));

                if (!string.Equals(record.TenantId, tenantId, StringComparison.Ordinal))
                    throw new ArgumentException("All queue event records in a batch must belong to the same tenant.", nameof(records));
            }

            return tenantId!;
        }

        private static string ValidateAppendRecord(QueueEventRecord record, string paramName)
        {
            if (record == null)
                throw new ArgumentException("Queue event record cannot be null.", paramName);

            if (string.IsNullOrWhiteSpace(record.TenantId))
                throw new ArgumentException("TenantId cannot be empty", paramName);

            ValidateTenantIdForPath(record.TenantId, paramName);

            if (string.IsNullOrWhiteSpace(record.FileKey))
                throw new ArgumentException("FileKey cannot be empty", paramName);

            return record.TenantId;
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
                QueueJournalMetrics.RecordCorruptTailAutoRepaired();

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

            var newlyDirty = _dirtyStateTenants.TryAdd(tenantId, 0);
            if (_disposed || _stateFlushTimer == null)
                return;

            if (!newlyDirty)
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
                        QueueJournalMetrics.RecordCorruptTailDetected();
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
            private readonly IReadOnlyList<QueueEventRecord>? _records;
            private readonly QueueEventRecord? _singleRecord;
            private IReadOnlyList<QueueEventRecord>? _singleRecordWrapper;

            public PendingAppendRequest(QueueEventRecord record, int estimatedBytes, bool createCompletion)
            {
                _singleRecord = record ?? throw new ArgumentNullException(nameof(record));
                EstimatedBytes = estimatedBytes;
                Completion = createCompletion
                    ? new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously)
                    : null;
            }

            public PendingAppendRequest(IReadOnlyList<QueueEventRecord> records, int estimatedBytes, bool createCompletion)
            {
                _records = records ?? throw new ArgumentNullException(nameof(records));
                EstimatedBytes = estimatedBytes;
                Completion = createCompletion
                    ? new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously)
                    : null;
            }

            public IReadOnlyList<QueueEventRecord> Records
            {
                get
                {
                    if (_records != null)
                        return _records;

                    if (_singleRecordWrapper == null)
                        _singleRecordWrapper = new[] { _singleRecord! };

                    return _singleRecordWrapper;
                }
            }

            public int RecordCount => _singleRecord != null ? 1 : _records!.Count;

            public int EstimatedBytes { get; }

            public TaskCompletionSource<object?>? Completion { get; }

            public bool TryGetSingleRecord(out QueueEventRecord record)
            {
                if (_singleRecord != null)
                {
                    record = _singleRecord;
                    return true;
                }

                record = null!;
                return false;
            }

            public void TrySetCompleted()
            {
                Completion?.TrySetResult(null);
            }

            public void TrySetException(Exception exception)
            {
                Completion?.TrySetException(exception);
            }
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

            public int WorkerStarted;

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

            foreach (var tenantWriter in _tenantWriters.Values)
                CloseTenantAppendStream(tenantWriter);

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
