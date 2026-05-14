using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class BackgroundCleanupServiceTests
    {
        [Fact]
        public async Task ExecuteAsync_RunsCleanupTasks_WhenEnabled()
        {
            var cleanupService = new Mock<IStorageCleanupService>(MockBehavior.Strict);
            var fileScheduler = new Mock<IFileScheduler>(MockBehavior.Strict);
            var logger = new Mock<ILogger<BackgroundCleanupService>>();
            using var cts = new CancellationTokenSource();

            cleanupService
                .Setup(s => s.CleanupAllEmptyDirectoriesAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            fileScheduler
                .Setup(s => s.CleanupOrphanedMetadataAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync(0);
            cleanupService
                .Setup(s => s.CleanupCompletedFilesAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            cleanupService
                .Setup(s => s.CleanupFilesByStatusAsync(null, null, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            cleanupService
                .Setup(s => s.CleanupInvalidDatabaseFilesAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((1, 1024L));
            cleanupService
                .Setup(s => s.GetCleanupStatisticsAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(ct =>
                {
                    cts.Cancel();
                    return Task.FromResult(new CleanupStatistics());
                });

            var service = new TestBackgroundCleanupService(
                cleanupService.Object,
                fileScheduler.Object,
                new CleanupOptions
                {
                    InitialDelay = TimeSpan.Zero,
                    CleanupInterval = TimeSpan.FromHours(1),
                    OptimizeDatabases = false,
                    CleanupTimedOutFiles = false,
                    PermanentlyFailedDisposition = PermanentlyFailedDisposition.Keep,
                    CleanupInvalidDatabaseBackups = true
                },
                logger.Object);

            await service.RunAsync(cts.Token);

            cleanupService.Verify(s => s.CleanupAllEmptyDirectoriesAsync(It.IsAny<CancellationToken>()), Times.Once);
            fileScheduler.Verify(s => s.CleanupOrphanedMetadataAsync(It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.CleanupCompletedFilesAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.CleanupFilesByStatusAsync(null, null, It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.CleanupInvalidDatabaseFilesAsync(It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.GetCleanupStatisticsAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task ExecuteAsync_RunsCompletedCleanupBeforeOrphanedMetadataCleanup()
        {
            var callOrder = new List<string>();
            var cleanupService = new Mock<IStorageCleanupService>(MockBehavior.Strict);
            var fileScheduler = new Mock<IFileScheduler>(MockBehavior.Strict);
            var logger = new Mock<ILogger<BackgroundCleanupService>>();
            using var cts = new CancellationTokenSource();

            cleanupService
                .Setup(s => s.CleanupAllEmptyDirectoriesAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            cleanupService
                .Setup(s => s.CleanupCompletedFilesAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .Callback(() => callOrder.Add("completed"))
                .Returns(Task.CompletedTask);
            fileScheduler
                .Setup(s => s.CleanupOrphanedMetadataAsync(It.IsAny<CancellationToken>()))
                .Callback(() => callOrder.Add("orphaned"))
                .ReturnsAsync(0);
            cleanupService
                .Setup(s => s.CleanupFilesByStatusAsync(null, null, It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            cleanupService
                .Setup(s => s.GetCleanupStatisticsAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(ct =>
                {
                    cts.Cancel();
                    return Task.FromResult(new CleanupStatistics());
                });

            var service = new TestBackgroundCleanupService(
                cleanupService.Object,
                fileScheduler.Object,
                new CleanupOptions
                {
                    InitialDelay = TimeSpan.Zero,
                    CleanupInterval = TimeSpan.FromHours(1),
                    OptimizeDatabases = false,
                    CleanupTimedOutFiles = false,
                    PermanentlyFailedDisposition = PermanentlyFailedDisposition.Keep,
                    CleanupInvalidDatabaseBackups = false
                },
                logger.Object);

            await service.RunAsync(cts.Token);

            var completedIndex = callOrder.IndexOf("completed");
            var orphanedIndex = callOrder.IndexOf("orphaned");
            Assert.True(completedIndex >= 0);
            Assert.True(orphanedIndex >= 0);
            Assert.True(completedIndex < orphanedIndex);
        }

        [Fact]
        public async Task ExecuteAsync_SkipsCleanupTasks_WhenDisabled()
        {
            var cleanupService = new Mock<IStorageCleanupService>(MockBehavior.Strict);
            var fileScheduler = new Mock<IFileScheduler>(MockBehavior.Strict);
            var logger = new Mock<ILogger<BackgroundCleanupService>>();

            var service = new TestBackgroundCleanupService(
                cleanupService.Object,
                fileScheduler.Object,
                new CleanupOptions
                {
                    Enabled = false,
                    InitialDelay = TimeSpan.Zero
                },
                logger.Object);

            await service.RunAsync(CancellationToken.None);

            cleanupService.VerifyNoOtherCalls();
            fileScheduler.VerifyNoOtherCalls();
        }

        private sealed class TestBackgroundCleanupService : BackgroundCleanupService
        {
            public TestBackgroundCleanupService(
                IStorageCleanupService cleanupService,
                IFileScheduler fileScheduler,
                CleanupOptions options,
                ILogger<BackgroundCleanupService> logger)
                : base(cleanupService, fileScheduler, options, logger)
            {
            }

            public Task RunAsync(CancellationToken cancellationToken)
            {
                return ExecuteAsync(cancellationToken);
            }
        }
    }
}
