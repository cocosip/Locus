using System;
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
        public async Task ExecuteAsync_RunsOrphanAndInvalidBackupCleanup_WhenEnabled()
        {
            var cleanupService = new Mock<IStorageCleanupService>(MockBehavior.Strict);
            var logger = new Mock<ILogger<BackgroundCleanupService>>();
            using var cts = new CancellationTokenSource();

            cleanupService
                .Setup(s => s.CleanupAllEmptyDirectoriesAsync(It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            cleanupService
                .Setup(s => s.CleanupAllOrphanedFilesAsync(It.IsAny<CancellationToken>()))
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
                new CleanupOptions
                {
                    InitialDelay = TimeSpan.Zero,
                    CleanupInterval = TimeSpan.FromHours(1),
                    OptimizeDatabases = false,
                    CleanupTimedOutFiles = false,
                    CleanupPermanentlyFailedFiles = false,
                    CleanupOrphanedFiles = true,
                    CleanupInvalidDatabaseBackups = true
                },
                logger.Object);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.RunAsync(cts.Token));

            cleanupService.Verify(s => s.CleanupAllEmptyDirectoriesAsync(It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.CleanupAllOrphanedFilesAsync(It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.CleanupFilesByStatusAsync(null, null, It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.CleanupInvalidDatabaseFilesAsync(It.IsAny<CancellationToken>()), Times.Once);
            cleanupService.Verify(s => s.GetCleanupStatisticsAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        private sealed class TestBackgroundCleanupService : BackgroundCleanupService
        {
            public TestBackgroundCleanupService(
                IStorageCleanupService cleanupService,
                CleanupOptions options,
                ILogger<BackgroundCleanupService> logger)
                : base(cleanupService, options, logger)
            {
            }

            public Task RunAsync(CancellationToken cancellationToken)
            {
                return ExecuteAsync(cancellationToken);
            }
        }
    }
}
