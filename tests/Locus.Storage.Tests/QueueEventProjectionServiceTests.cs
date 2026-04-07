using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class QueueEventProjectionServiceTests : IDisposable
    {
        private readonly IFileSystem _fileSystem;
        private readonly List<IDisposable> _trackedDisposables;
        private readonly string _rootDirectory;
        private readonly string _metadataDirectory;
        private readonly string _quotaDirectory;
        private readonly string _queueDirectory;
        private readonly string _volumeDirectory;
        private readonly MetadataRepository _metadataRepository;
        private readonly DirectoryQuotaRepository _quotaRepository;

        public QueueEventProjectionServiceTests()
        {
            _fileSystem = new System.IO.Abstractions.FileSystem();
            _trackedDisposables = new List<IDisposable>();
            _rootDirectory = Path.Combine(Path.GetTempPath(), $"locus-test-projector-{Guid.NewGuid():N}");
            _metadataDirectory = Path.Combine(_rootDirectory, "metadata");
            _quotaDirectory = Path.Combine(_rootDirectory, "quota");
            _queueDirectory = Path.Combine(_rootDirectory, "queue");
            _volumeDirectory = Path.Combine(_rootDirectory, "volume");

            _fileSystem.Directory.CreateDirectory(_metadataDirectory);
            _fileSystem.Directory.CreateDirectory(_quotaDirectory);
            _fileSystem.Directory.CreateDirectory(_queueDirectory);
            _fileSystem.Directory.CreateDirectory(_volumeDirectory);

            _metadataRepository = new MetadataRepository(
                _fileSystem,
                new Mock<ILogger<MetadataRepository>>().Object,
                _metadataDirectory,
                enableBackgroundPersistence: false);
            _quotaRepository = new DirectoryQuotaRepository(
                _fileSystem,
                new Mock<ILogger<DirectoryQuotaRepository>>().Object,
                _quotaDirectory,
                enableBackgroundFlush: false);
        }

        [Fact]
        public async Task ExecuteAsync_RebuildsAcceptedEventIntoMetadataAndQuota()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-001";
            const string fileKey = "file-001";
            const string directoryPath = "/incoming";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "incoming", "file-001.dcm");
            var fileBytes = new byte[] { 1, 2, 3, 4 };

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, fileBytes);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = fileBytes.Length,
                Status = FileProcessingStatus.Pending,
                RetryCount = 0,
                OriginalFileName = "scan.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            var service = TrackDisposable(new QueueEventProjectionService(
                journal,
                _metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                _fileSystem,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    Enabled = true,
                    EnableProjection = true,
                    MaxRecordsPerTenantPerCycle = 16,
                    MaxTenantsPerCycle = 4,
                    BusyCycleDelay = TimeSpan.FromMilliseconds(10),
                    IdleCycleDelay = TimeSpan.FromMilliseconds(10),
                    MaxProjectionTimePerCycle = TimeSpan.FromSeconds(1)
                },
                new Mock<ILogger<QueueEventProjectionService>>().Object));

            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(async () =>
                {
                    var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
                    return metadata != null;
                }, TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            var tenantCount = await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None);
            var directoryCount = await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None);
            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");

            Assert.NotNull(rebuilt);
            Assert.Equal(fileKey, rebuilt!.FileKey);
            Assert.Equal(physicalPath, rebuilt.PhysicalPath);
            Assert.Equal(".dcm", rebuilt.FileExtension);
            Assert.Equal(1, tenantCount);
            Assert.Equal(1, directoryCount);
            Assert.True(_fileSystem.File.Exists(cursorPath));
        }

        [Fact]
        public async Task ExecuteAsync_ProjectsFailedEventWithRetryDelay()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-002";
            const string fileKey = "file-002";
            const string directoryPath = "/incoming";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "incoming", "file-002.dcm");
            var retryAt = DateTime.UtcNow.AddMinutes(2);

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 7, 8, 9 });

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(-10),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending,
                OriginalFileName = "scan2.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            var processingStartTime = DateTime.UtcNow.AddSeconds(-5);
            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = processingStartTime,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = processingStartTime
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending,
                RetryCount = 1,
                AvailableForProcessingAtUtc = retryAt,
                ErrorMessage = "decode failed",
                OriginalFileName = "scan2.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);
            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(async () =>
                {
                    var metadata = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
                    return metadata != null && metadata.Status == FileProcessingStatus.Pending && metadata.RetryCount == 1;
                }, TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            var tenantCount = await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None);
            var directoryCount = await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None);

            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Pending, rebuilt!.Status);
            Assert.Equal(1, rebuilt.RetryCount);
            Assert.Equal("decode failed", rebuilt.LastError);
            Assert.Equal(retryAt, rebuilt.AvailableForProcessingAt);
            Assert.Null(rebuilt.ProcessingStartTime);
            Assert.Equal(1, tenantCount);
            Assert.Equal(1, directoryCount);
        }

        [Fact]
        public async Task ExecuteAsync_ProjectsCompletedEventIntoCompletedState()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-003";
            const string fileKey = "file-003";
            const string directoryPath = "/done";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "done", "file-003.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 1, 2, 3, 4, 5 });

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(-20),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 5,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            var processingStartTime = DateTime.UtcNow.AddSeconds(-10);
            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = processingStartTime,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 5,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = processingStartTime
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingCompleted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 5,
                Status = FileProcessingStatus.Completed,
                ProcessingStartTimeUtc = processingStartTime
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);
            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");
            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(() => Task.FromResult(_fileSystem.File.Exists(cursorPath)), TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            var tenantCount = await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None);
            var directoryCount = await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None);

            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Completed, rebuilt!.Status);
            Assert.NotNull(rebuilt.CompletedAt);
            Assert.Equal(1, tenantCount);
            Assert.Equal(1, directoryCount);
        }

        [Fact]
        public async Task ExecuteAsync_DeleteRequestedPromotesProjectionToCompletedState()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-003b";
            const string fileKey = "file-003b";
            const string directoryPath = "/done";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "done", "file-003b.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 1, 2, 3, 4, 5 });

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(-20),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 5,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            var processingStartTime = DateTime.UtcNow.AddSeconds(-10);
            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = processingStartTime,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 5,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = processingStartTime
            }, CancellationToken.None);

            var deleteRequestedAt = DateTime.UtcNow;
            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.DeleteRequested,
                OccurredAtUtc = deleteRequestedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 5,
                Status = FileProcessingStatus.Completed
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);
            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");
            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(() => Task.FromResult(_fileSystem.File.Exists(cursorPath)), TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);

            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Completed, rebuilt!.Status);
            Assert.Null(rebuilt.ProcessingStartTime);
            Assert.NotNull(rebuilt.CompletedAt);
            Assert.True(rebuilt.CompletedAt.Value >= processingStartTime);
        }

        [Fact]
        public async Task ExecuteAsync_ReplaysEndToEndLifecycleToFinalDeleteSucceededState()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-004";
            const string fileKey = "file-004";
            const string directoryPath = "/dicom";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "dicom", "file-004.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 9, 8, 7, 6 });

            var acceptedAt = DateTime.UtcNow.AddSeconds(-30);
            var firstProcessingStart = DateTime.UtcNow.AddSeconds(-20);
            var retryAt = DateTime.UtcNow.AddSeconds(-5);
            var secondProcessingStart = DateTime.UtcNow.AddSeconds(-2);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = acceptedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Pending,
                OriginalFileName = "study.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = firstProcessingStart,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = firstProcessingStart
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(-10),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Pending,
                RetryCount = 1,
                AvailableForProcessingAtUtc = retryAt,
                ErrorMessage = "temporary failure",
                OriginalFileName = "study.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = secondProcessingStart,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = secondProcessingStart
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingCompleted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Completed,
                ProcessingStartTimeUtc = secondProcessingStart,
                OriginalFileName = "study.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            _fileSystem.File.Delete(physicalPath);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.DeleteSucceeded,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(1),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Completed,
                OriginalFileName = "study.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);
            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");
            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(() => Task.FromResult(_fileSystem.File.Exists(cursorPath)), TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);
            var tenantCount = await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None);
            var directoryCount = await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None);

            Assert.Null(rebuilt);
            Assert.Equal(0, tenantCount);
            Assert.Equal(0, directoryCount);
        }

        [Fact]
        public async Task ExecuteAsync_IgnoresStaleStartedReplayAfterFailure()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-005";
            const string fileKey = "file-005";
            const string directoryPath = "/dicom";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "dicom", "file-005.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 1, 1, 1 });

            var startedAt = DateTime.UtcNow.AddSeconds(-20);
            var failedAt = DateTime.UtcNow.AddSeconds(-10);
            var retryAt = DateTime.UtcNow.AddMinutes(1);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(-30),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = startedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = startedAt
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = failedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending,
                RetryCount = 1,
                AvailableForProcessingAtUtc = retryAt,
                ErrorMessage = "temporary failure"
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = startedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = startedAt
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);
            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");
            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(() => Task.FromResult(_fileSystem.File.Exists(cursorPath)), TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);

            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Pending, rebuilt!.Status);
            Assert.Equal(1, rebuilt.RetryCount);
            Assert.Equal(failedAt, rebuilt.LastFailedAt);
            Assert.Null(rebuilt.ProcessingStartTime);
        }

        [Fact]
        public async Task ExecuteAsync_IgnoresStaleFailedReplayAfterNewerStarted()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-006";
            const string fileKey = "file-006";
            const string directoryPath = "/dicom";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "dicom", "file-006.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 2, 2, 2 });

            var firstStartedAt = DateTime.UtcNow.AddSeconds(-20);
            var failureAt = DateTime.UtcNow.AddSeconds(-15);
            var secondStartedAt = DateTime.UtcNow.AddSeconds(-5);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow.AddSeconds(-30),
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = firstStartedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = firstStartedAt
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = failureAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending,
                RetryCount = 1,
                AvailableForProcessingAtUtc = DateTime.UtcNow.AddMinutes(1),
                ErrorMessage = "temporary failure",
                ProcessingStartTimeUtc = firstStartedAt
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = secondStartedAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = secondStartedAt
            }, CancellationToken.None);

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = failureAt,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending,
                RetryCount = 1,
                AvailableForProcessingAtUtc = DateTime.UtcNow.AddMinutes(1),
                ErrorMessage = "temporary failure",
                ProcessingStartTimeUtc = firstStartedAt
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);
            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");
            await service.StartAsync(CancellationToken.None);
            try
            {
                await WaitUntilAsync(() => Task.FromResult(_fileSystem.File.Exists(cursorPath)), TimeSpan.FromSeconds(2));
            }
            finally
            {
                await service.StopAsync(CancellationToken.None);
            }

            var rebuilt = await _metadataRepository.GetByFileKeyAsync(fileKey, CancellationToken.None);

            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Processing, rebuilt!.Status);
            Assert.Equal(secondStartedAt, rebuilt.ProcessingStartTime);
        }

        [Fact]
        public async Task ReplayTenantAsync_DrainsLagWithoutStartingHostedLoop()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var journal = CreateJournal();

            const string tenantId = "tenant-007";
            const string fileKey = "file-007";
            const string directoryPath = "/replay";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "replay", "file-007.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 4, 4, 4 });

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            var service = CreateProjectionService(journal, tenantQuotaManager, directoryQuotaManager);

            var before = await service.GetTenantStateAsync(tenantId, CancellationToken.None);
            Assert.True(before.LagBytes > 0);

            var after = await service.ReplayTenantAsync(tenantId, CancellationToken.None);
            var rebuilt = await _metadataRepository.GetAsync(tenantId, fileKey, CancellationToken.None);

            Assert.NotNull(rebuilt);
            Assert.Equal(0, after.LagBytes);
            Assert.True(after.JournalSizeBytes >= after.CursorOffset);
        }

        [Fact]
        public async Task RebuildTenantAsync_RemovesStaleProjectionAndReconcilesQuota()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var cleanupService = CreateCleanupService(tenantQuotaManager);
            var journal = CreateJournal();

            const string tenantId = "tenant-008";
            const string fileKey = "file-008";
            const string directoryPath = "/incoming";
            const string staleFileKey = "stale-008";
            const string staleDirectoryPath = "/stale";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "incoming", "file-008.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 8, 8, 8 });

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                TenantId = tenantId,
                FileKey = staleFileKey,
                VolumeId = "vol-001",
                PhysicalPath = Path.Combine(_volumeDirectory, tenantId, "stale", "stale-008.dcm"),
                DirectoryPath = staleDirectoryPath,
                FileSize = 1,
                CreatedAt = DateTime.UtcNow,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            await _quotaRepository.SetCurrentCountAsync(tenantId, tenantId, 5, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync(tenantId, directoryPath, 3, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync(tenantId, staleDirectoryPath, 2, CancellationToken.None);

            var service = TrackDisposable(new QueueEventProjectionService(
                journal,
                _metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                _fileSystem,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    Enabled = true,
                    EnableProjection = true,
                    MaxRecordsPerTenantPerCycle = 16,
                    MaxTenantsPerCycle = 4,
                    BusyCycleDelay = TimeSpan.FromMilliseconds(10),
                    IdleCycleDelay = TimeSpan.FromMilliseconds(10),
                    MaxProjectionTimePerCycle = TimeSpan.FromSeconds(1)
                },
                new Mock<ILogger<QueueEventProjectionService>>().Object,
                cleanupService));

            var state = await service.RebuildTenantAsync(tenantId, CancellationToken.None);

            Assert.Equal(0, state.LagBytes);
            Assert.NotNull(await _metadataRepository.GetAsync(tenantId, fileKey, CancellationToken.None));
            Assert.Null(await _metadataRepository.GetAsync(tenantId, staleFileKey, CancellationToken.None));
            Assert.Equal(1, await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None));
            Assert.Equal(1, await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None));
            Assert.Equal(0, await directoryQuotaManager.GetFileCountAsync(tenantId, staleDirectoryPath, CancellationToken.None));
        }

        [Fact]
        public async Task SnapshotTenantAsync_AllowsRebuildToRestoreProjectionWithoutJournalReplay()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var cleanupService = CreateCleanupService(tenantQuotaManager);
            var journal = CreateJournal();

            const string tenantId = "tenant-009";
            const string fileKey = "file-009";
            const string directoryPath = "/snapshot";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "snapshot", "file-009.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 9, 9, 9 });

            await _metadataRepository.AddOrUpdateAsync(new FileMetadata
            {
                TenantId = tenantId,
                FileKey = fileKey,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 3,
                CreatedAt = DateTime.UtcNow.AddMinutes(-1),
                Status = FileProcessingStatus.Pending,
                OriginalFileName = "snapshot.dcm",
                FileExtension = ".dcm"
            }, CancellationToken.None);

            var service = TrackDisposable(new QueueEventProjectionService(
                journal,
                _metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                _fileSystem,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    Enabled = true,
                    EnableProjection = true,
                    MaxRecordsPerTenantPerCycle = 16,
                    MaxTenantsPerCycle = 4,
                    BusyCycleDelay = TimeSpan.FromMilliseconds(10),
                    IdleCycleDelay = TimeSpan.FromMilliseconds(10),
                    MaxProjectionTimePerCycle = TimeSpan.FromSeconds(1)
                },
                new Mock<ILogger<QueueEventProjectionService>>().Object,
                cleanupService));

            var snapshotState = await service.SnapshotTenantAsync(tenantId, CancellationToken.None);

            Assert.True(snapshotState.HasSnapshot);
            Assert.Equal(1, snapshotState.SnapshotFileCount);

            await _metadataRepository.RemoveAsync(tenantId, fileKey, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync(tenantId, tenantId, 0, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync(tenantId, directoryPath, 0, CancellationToken.None);

            var rebuiltState = await service.RebuildTenantAsync(tenantId, CancellationToken.None);

            Assert.True(rebuiltState.HasSnapshot);
            Assert.Equal(0, rebuiltState.LagBytes);

            var rebuilt = await _metadataRepository.GetAsync(tenantId, fileKey, CancellationToken.None);
            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Pending, rebuilt!.Status);
            Assert.Equal(1, await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None));
            Assert.Equal(1, await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None));
        }

        [Fact]
        public async Task ReplayTenantAsync_WithSnapshotAndCompaction_AllowsRebuildFromCompactedJournal()
        {
            var tenantQuotaManager = CreateTenantQuotaManager();
            var directoryQuotaManager = CreateDirectoryQuotaManager();
            var cleanupService = CreateCleanupService(tenantQuotaManager);
            var journal = CreateJournal();

            const string tenantId = "tenant-010";
            const string fileKey = "file-010";
            const string directoryPath = "/compact";
            var physicalPath = Path.Combine(_volumeDirectory, tenantId, "compact", "file-010.dcm");

            _fileSystem.Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);
            _fileSystem.File.WriteAllBytes(physicalPath, new byte[] { 1, 0, 1, 0 });

            await journal.AppendAsync(new QueueEventRecord
            {
                TenantId = tenantId,
                FileKey = fileKey,
                EventType = QueueEventType.Accepted,
                OccurredAtUtc = DateTime.UtcNow,
                VolumeId = "vol-001",
                PhysicalPath = physicalPath,
                DirectoryPath = directoryPath,
                FileSize = 4,
                Status = FileProcessingStatus.Pending
            }, CancellationToken.None);

            var service = TrackDisposable(new QueueEventProjectionService(
                journal,
                _metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                _fileSystem,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    Enabled = true,
                    EnableProjection = true,
                    EnableCompaction = true,
                    MinBytesBeforeCompaction = 1,
                    MaxRecordsPerTenantPerCycle = 16,
                    MaxTenantsPerCycle = 4,
                    BusyCycleDelay = TimeSpan.FromMilliseconds(10),
                    IdleCycleDelay = TimeSpan.FromMilliseconds(10),
                    MaxProjectionTimePerCycle = TimeSpan.FromSeconds(1)
                },
                new Mock<ILogger<QueueEventProjectionService>>().Object,
                cleanupService));

            var replayState = await service.ReplayTenantAsync(tenantId, CancellationToken.None);
            var baseOffset = await journal.GetBaseOffsetAsync(tenantId, CancellationToken.None);

            Assert.True(replayState.HasSnapshot);
            Assert.True(baseOffset > 0);

            var cursorPath = Path.Combine(_queueDirectory, tenantId, "projector.cursor.json");
            _fileSystem.File.WriteAllText(cursorPath, "{\"offset\":0}");

            var recoveredState = await service.ReplayTenantAsync(tenantId, CancellationToken.None);

            Assert.Equal(baseOffset, recoveredState.CursorOffset);
            Assert.Equal(0, recoveredState.LagBytes);

            await _metadataRepository.RemoveAsync(tenantId, fileKey, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync(tenantId, tenantId, 0, CancellationToken.None);
            await _quotaRepository.SetCurrentCountAsync(tenantId, directoryPath, 0, CancellationToken.None);

            var rebuiltState = await service.RebuildTenantAsync(tenantId, CancellationToken.None);
            var rebuilt = await _metadataRepository.GetAsync(tenantId, fileKey, CancellationToken.None);

            Assert.True(rebuiltState.HasSnapshot);
            Assert.Equal(0, rebuiltState.LagBytes);
            Assert.NotNull(rebuilt);
            Assert.Equal(FileProcessingStatus.Pending, rebuilt!.Status);
            Assert.Equal(1, await tenantQuotaManager.GetFileCountAsync(tenantId, CancellationToken.None));
            Assert.Equal(1, await directoryQuotaManager.GetFileCountAsync(tenantId, directoryPath, CancellationToken.None));
        }

        public void Dispose()
        {
            foreach (var disposable in _trackedDisposables)
            {
                try
                {
                    disposable.Dispose();
                }
                catch
                {
                }
            }

            _metadataRepository.Dispose();
            _quotaRepository.Dispose();
            SqliteConnection.ClearAllPools();

            try
            {
                if (_fileSystem.Directory.Exists(_rootDirectory))
                    _fileSystem.Directory.Delete(_rootDirectory, recursive: true);
            }
            catch
            {
            }
        }

        private static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < timeout)
            {
                if (await condition().ConfigureAwait(false))
                    return;

                await Task.Delay(25).ConfigureAwait(false);
            }

            throw new TimeoutException("Condition was not satisfied within the expected timeout.");
        }

        private TenantQuotaManager CreateTenantQuotaManager()
        {
            return new TenantQuotaManager(
                _quotaRepository,
                new Mock<ILogger<TenantQuotaManager>>().Object);
        }

        private DirectoryQuotaManager CreateDirectoryQuotaManager()
        {
            return new DirectoryQuotaManager(
                _quotaRepository,
                new Mock<ILogger<DirectoryQuotaManager>>().Object);
        }

        private StorageCleanupService CreateCleanupService(TenantQuotaManager tenantQuotaManager)
        {
            return new StorageCleanupService(
                _metadataRepository,
                _quotaRepository,
                tenantQuotaManager,
                _fileSystem,
                new Mock<ILogger<StorageCleanupService>>().Object,
                _metadataDirectory,
                _quotaDirectory);
        }

        private FileQueueEventJournal CreateJournal()
        {
            return new FileQueueEventJournal(
                _fileSystem,
                new Mock<ILogger<FileQueueEventJournal>>().Object,
                _queueDirectory);
        }

        private QueueEventProjectionService CreateProjectionService(
            TenantQuotaManager tenantQuotaManager,
            DirectoryQuotaManager directoryQuotaManager)
        {
            return CreateProjectionService(CreateJournal(), tenantQuotaManager, directoryQuotaManager);
        }

        private QueueEventProjectionService CreateProjectionService(
            FileQueueEventJournal journal,
            TenantQuotaManager tenantQuotaManager,
            DirectoryQuotaManager directoryQuotaManager)
        {
            return TrackDisposable(new QueueEventProjectionService(
                journal,
                _metadataRepository,
                tenantQuotaManager,
                directoryQuotaManager,
                _fileSystem,
                new QueueEventJournalOptions
                {
                    QueueDirectory = _queueDirectory,
                    Enabled = true,
                    EnableProjection = true,
                    MaxRecordsPerTenantPerCycle = 16,
                    MaxTenantsPerCycle = 4,
                    BusyCycleDelay = TimeSpan.FromMilliseconds(10),
                    IdleCycleDelay = TimeSpan.FromMilliseconds(10),
                    MaxProjectionTimePerCycle = TimeSpan.FromSeconds(1)
                },
                new Mock<ILogger<QueueEventProjectionService>>().Object));
        }

        private T TrackDisposable<T>(T disposable)
            where T : IDisposable
        {
            _trackedDisposables.Add(disposable);
            return disposable;
        }
    }
}
