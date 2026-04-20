#nullable enable
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Abstractions;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage;

namespace Locus.Benchmarks
{
    public enum QueueLogEncodingFormat
    {
        Json = 0,
        Binary = 1,
    }

    internal interface IQueueLogBenchmarkJournal : IDisposable
    {
        Task AppendAsync(QueueEventRecord record, CancellationToken cancellationToken);
    }

    internal sealed class QueueLogBenchmarkJournal : IQueueLogBenchmarkJournal
    {
        private static readonly Encoding Utf8NoBom = new UTF8Encoding(false);
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false,
        };
        private static readonly TimeSpan QueuePollInterval = TimeSpan.FromMilliseconds(1);
        private const int BinaryRecordMagic = 0x474F4C51;

        private readonly IFileSystem _fileSystem;
        private readonly string _queueDirectory;
        private readonly QueueLogEncodingFormat _format;
        private readonly QueueEventJournalAckMode _ackMode;
        private readonly TimeSpan _linger;
        private readonly TimeSpan _writerIdleTimeout;
        private readonly TimeSpan _balancedFlushWindow;
        private readonly int _maxBatchRecords;
        private readonly int _maxBatchBytes;
        private readonly ConcurrentDictionary<string, TenantWriterState> _tenantWriters;
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _appendLocks;
        private bool _disposed;

        public QueueLogBenchmarkJournal(
            IFileSystem fileSystem,
            string queueDirectory,
            QueueLogEncodingFormat format,
            QueueEventJournalAckMode ackMode,
            TimeSpan linger,
            int maxBatchRecords,
            int maxBatchBytes,
            TimeSpan writerIdleTimeout,
            TimeSpan balancedFlushWindow)
        {
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _queueDirectory = queueDirectory ?? throw new ArgumentNullException(nameof(queueDirectory));
            _format = format;
            _ackMode = ackMode;
            _linger = linger;
            _maxBatchRecords = maxBatchRecords;
            _maxBatchBytes = maxBatchBytes;
            _writerIdleTimeout = writerIdleTimeout;
            _balancedFlushWindow = balancedFlushWindow;
            _tenantWriters = new ConcurrentDictionary<string, TenantWriterState>(StringComparer.Ordinal);
            _appendLocks = new ConcurrentDictionary<string, SemaphoreSlim>(StringComparer.Ordinal);

            if (!_fileSystem.Directory.Exists(_queueDirectory))
                _fileSystem.Directory.CreateDirectory(_queueDirectory);
        }

        public async Task AppendAsync(QueueEventRecord record, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            if (record == null)
                throw new ArgumentNullException(nameof(record));

            ValidateRecord(record);
            cancellationToken.ThrowIfCancellationRequested();

            var request = new PendingAppendRequest(
                CloneRecord(record),
                EstimateRecordSize(record),
                createCompletion: _ackMode != QueueEventJournalAckMode.Async);
            GetOrCreateTenantWriter(record.TenantId).Enqueue(request);

            if (_ackMode == QueueEventJournalAckMode.Async)
                return;

            await request.Completion!.Task.ConfigureAwait(false);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            foreach (var tenantWriter in _tenantWriters.Values)
                tenantWriter.RequestShutdown();

            foreach (var tenantWriter in _tenantWriters.Values)
            {
                try
                {
                    tenantWriter.WorkerTask.GetAwaiter().GetResult();
                }
                catch
                {
                }
            }
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
                    }
                }

                await FlushAppendStreamIfNeededAsync(writer).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (writer.ShutdownRequested)
            {
            }
            catch (Exception ex)
            {
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
                if (writer.PendingRequests.TryDequeue(out var request))
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
            var totalRecords = 1;
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
                && writer.PendingRequests.TryDequeue(out var nextRequest))
            {
                drainedAny = true;
                batch.Add(nextRequest);
                totalRecords++;
                totalBytes += nextRequest.EstimatedBytes;
            }

            return drainedAny;
        }

        private async Task WriteBatchAsync(TenantWriterState writer, IReadOnlyList<PendingAppendRequest> batch)
        {
            var appendLock = _appendLocks.GetOrAdd(writer.TenantId, _ => new SemaphoreSlim(1, 1));
            await appendLock.WaitAsync(writer.Cancellation.Token).ConfigureAwait(false);
            try
            {
                var stream = EnsureAppendStream(writer);
                var serializedRecords = new List<byte[]>(batch.Count);
                var totalBytes = 0;
                foreach (var request in batch)
                {
                    writer.LastSequenceNumber++;
                    request.Record.SequenceNumber = writer.LastSequenceNumber;

                    var bytes = _format == QueueLogEncodingFormat.Json
                        ? SerializeJsonRecord(request.Record)
                        : SerializeBinaryRecord(request.Record);
                    serializedRecords.Add(bytes);
                    totalBytes += bytes.Length;
                }

                var writeBuffer = ArrayPool<byte>.Shared.Rent(totalBytes);
                try
                {
                    var writeOffset = 0;
                    foreach (var bytes in serializedRecords)
                    {
                        Buffer.BlockCopy(bytes, 0, writeBuffer, writeOffset, bytes.Length);
                        writeOffset += bytes.Length;
                    }

                    await stream.WriteAsync(writeBuffer, 0, totalBytes, writer.Cancellation.Token).ConfigureAwait(false);
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(writeBuffer);
                }

                if (ShouldFlushBeforeAck(writer))
                    await FlushAppendStreamCoreAsync(writer).ConfigureAwait(false);
                else
                    writer.HasUnflushedData = true;

                CompleteRequests(batch);
            }
            finally
            {
                appendLock.Release();
            }
        }

        private Stream EnsureAppendStream(TenantWriterState writer)
        {
            if (writer.AppendStream != null)
                return writer.AppendStream;

            var tenantDirectory = _fileSystem.Path.Combine(_queueDirectory, writer.TenantId);
            if (!_fileSystem.Directory.Exists(tenantDirectory))
                _fileSystem.Directory.CreateDirectory(tenantDirectory);

            writer.AppendStream = _fileSystem.File.Open(
                GetLogPath(writer.TenantId),
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read);
            writer.LastFlushUtc = DateTime.UtcNow;
            return writer.AppendStream;
        }

        private string GetLogPath(string tenantId)
        {
            var fileName = _format == QueueLogEncodingFormat.Json ? "queue.log" : "queue.binlog";
            return _fileSystem.Path.Combine(_queueDirectory, tenantId, fileName);
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
            if (writer.AppendStream != null)
            {
                await writer.AppendStream.FlushAsync(writer.Cancellation.Token).ConfigureAwait(false);
                if (writer.AppendStream is FileStream fileStream)
                    fileStream.Flush(true);
            }

            writer.HasUnflushedData = false;
            writer.LastFlushUtc = DateTime.UtcNow;
        }

        private byte[] SerializeJsonRecord(QueueEventRecord record)
        {
            record.PayloadCrc32 = ComputeJsonPayloadChecksum(record);
            var line = JsonSerializer.Serialize(record, JsonOptions) + "\n";
            return Utf8NoBom.GetBytes(line);
        }

        private byte[] SerializeBinaryRecord(QueueEventRecord record)
        {
            var payload = BuildBinaryPayload(record);
            var checksum = QueueLogBenchmarkCrc32.Compute(payload);
            record.PayloadCrc32 = checksum;

            using (var stream = new MemoryStream(payload.Length + 12))
            using (var writer = new BinaryWriter(stream, Utf8NoBom, leaveOpen: true))
            {
                writer.Write(BinaryRecordMagic);
                writer.Write(payload.Length);
                writer.Write(checksum);
                writer.Write(payload);
                writer.Flush();
                return stream.ToArray();
            }
        }

        private byte[] BuildBinaryPayload(QueueEventRecord record)
        {
            using (var stream = new MemoryStream(512))
            using (var writer = new BinaryWriter(stream, Utf8NoBom, leaveOpen: true))
            {
                writer.Write(record.SchemaVersion);
                writer.Write(record.SequenceNumber ?? 0L);
                writer.Write((int)record.EventType);
                writer.Write(record.OccurredAtUtc.Ticks);
                writer.Write(record.FileSize ?? -1L);
                writer.Write(record.Status.HasValue ? (int)record.Status.Value : int.MinValue);
                writer.Write(record.RetryCount ?? -1);
                writer.Write(record.ProcessingStartTimeUtc?.Ticks ?? 0L);
                writer.Write(record.AvailableForProcessingAtUtc?.Ticks ?? 0L);
                WriteNullableString(writer, record.EventId);
                WriteNullableString(writer, record.TenantId);
                WriteNullableString(writer, record.FileKey);
                WriteNullableString(writer, record.VolumeId);
                WriteNullableString(writer, record.PhysicalPath);
                WriteNullableString(writer, record.DirectoryPath);
                WriteNullableString(writer, record.ErrorMessage);
                WriteNullableString(writer, record.OriginalFileName);
                WriteNullableString(writer, record.FileExtension);
                writer.Flush();
                return stream.ToArray();
            }
        }

        private static void WriteNullableString(BinaryWriter writer, string? value)
        {
            writer.Write(value != null);
            if (value != null)
                writer.Write(value);
        }

        private uint ComputeJsonPayloadChecksum(QueueEventRecord record)
        {
            var payload = CloneRecord(record);
            payload.PayloadCrc32 = null;
            var bytes = Utf8NoBom.GetBytes(JsonSerializer.Serialize(payload, JsonOptions));
            return QueueLogBenchmarkCrc32.Compute(bytes);
        }

        private static void ValidateRecord(QueueEventRecord record)
        {
            if (string.IsNullOrWhiteSpace(record.TenantId))
                throw new ArgumentException("TenantId cannot be empty.", nameof(record));

            if (string.IsNullOrWhiteSpace(record.FileKey))
                throw new ArgumentException("FileKey cannot be empty.", nameof(record));
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
            catch
            {
            }
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

        private void ThrowIfDisposed()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(QueueLogBenchmarkJournal));
        }

        private sealed class PendingAppendRequest
        {
            public PendingAppendRequest(QueueEventRecord record, int estimatedBytes, bool createCompletion)
            {
                Record = record ?? throw new ArgumentNullException(nameof(record));
                EstimatedBytes = estimatedBytes;
                Completion = createCompletion
                    ? new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously)
                    : null;
            }

            public QueueEventRecord Record { get; }

            public int EstimatedBytes { get; }

            public TaskCompletionSource<object?>? Completion { get; }

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

            public Stream? AppendStream { get; set; }

            public bool HasUnflushedData { get; set; }

            public DateTime LastFlushUtc { get; set; }

            public long LastSequenceNumber { get; set; }

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
    }
}
