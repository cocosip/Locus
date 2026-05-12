using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class ProcessingTimeoutRecoveryServiceTests
    {
        [Fact]
        public async Task RecoverTimedOutFilesAsync_WhenCancellationIsRequestedBeforeAppend_ThrowsAndDoesNotAppendTimeoutEvent()
        {
            const string tenantId = "tenant-cancel-timeout";
            var processingStartTime = DateTime.UtcNow.AddMinutes(-10);
            var metadata = new FileMetadata
            {
                TenantId = tenantId,
                FileKey = "file-001",
                VolumeId = "vol-001",
                PhysicalPath = "/data/file-001.dat",
                DirectoryPath = "/incoming",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStartTime,
                CreatedAt = processingStartTime,
            };

            using var cts = new CancellationTokenSource();

            var cleanupStore = new Mock<IQueueProjectionCleanupStore>(MockBehavior.Strict);
            cleanupStore
                .Setup(store => store.GetTimedOutProcessingFilesAsync(
                    tenantId,
                    It.IsAny<DateTime>(),
                    10,
                    cts.Token))
                .Callback(() => cts.Cancel())
                .ReturnsAsync(new[] { metadata });

            var coordinator = new ProcessingTimeoutRecoveryCoordinator();
            var journal = new Mock<IQueueEventJournal>(MockBehavior.Strict);

            var service = new ProcessingTimeoutRecoveryService(
                cleanupStore.Object,
                coordinator,
                Mock.Of<ILogger<ProcessingTimeoutRecoveryService>>(),
                journal.Object);

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                service.RecoverTimedOutFilesAsync(
                    tenantId,
                    10,
                    DateTime.UtcNow,
                    TimeSpan.FromMinutes(5),
                    cts.Token));

            journal.Verify(
                j => j.AppendAsync(It.IsAny<QueueEventRecord>(), It.IsAny<CancellationToken>()),
                Times.Never);
            cleanupStore.Verify(
                store => store.TryResetTimedOutFileAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<DateTime>(),
                    It.IsAny<DateTime>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
        }

        [Fact]
        public async Task RecoverTimedOutFilesAsync_WhenJournalAppendObservesCancellation_ThrowsAndDoesNotResetProjection()
        {
            const string tenantId = "tenant-cancel-timeout";
            var processingStartTime = DateTime.UtcNow.AddMinutes(-10);
            var metadata = new FileMetadata
            {
                TenantId = tenantId,
                FileKey = "file-001",
                VolumeId = "vol-001",
                PhysicalPath = "/data/file-001.dat",
                DirectoryPath = "/incoming",
                Status = FileProcessingStatus.Processing,
                ProcessingStartTime = processingStartTime,
                CreatedAt = processingStartTime,
            };

            using var cts = new CancellationTokenSource();

            var cleanupStore = new Mock<IQueueProjectionCleanupStore>(MockBehavior.Strict);
            cleanupStore
                .Setup(store => store.GetTimedOutProcessingFilesAsync(
                    tenantId,
                    It.IsAny<DateTime>(),
                    10,
                    cts.Token))
                .ReturnsAsync(new[] { metadata });

            var coordinator = new ProcessingTimeoutRecoveryCoordinator();
            var journal = new Mock<IQueueEventJournal>(MockBehavior.Strict);
            journal.Verify(
                j => j.AppendAsync(It.IsAny<QueueEventRecord>(), It.IsAny<CancellationToken>()),
                Times.Never);
            journal
                .Setup(j => j.AppendAsync(It.IsAny<QueueEventRecord>(), cts.Token))
                .Callback(() => cts.Cancel())
                .ThrowsAsync(new OperationCanceledException(cts.Token));

            var service = new ProcessingTimeoutRecoveryService(
                cleanupStore.Object,
                coordinator,
                Mock.Of<ILogger<ProcessingTimeoutRecoveryService>>(),
                journal.Object);

            await Assert.ThrowsAsync<OperationCanceledException>(() =>
                service.RecoverTimedOutFilesAsync(
                    tenantId,
                    10,
                    DateTime.UtcNow,
                    TimeSpan.FromMinutes(5),
                    cts.Token));

            cleanupStore.Verify(
                store => store.TryResetTimedOutFileAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<DateTime>(),
                    It.IsAny<DateTime>(),
                    It.IsAny<CancellationToken>()),
                Times.Never);
            journal.Verify(
                j => j.AppendAsync(It.IsAny<QueueEventRecord>(), cts.Token),
                Times.Once);
        }
    }
}
