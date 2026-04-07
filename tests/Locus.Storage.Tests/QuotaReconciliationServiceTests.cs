using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Storage;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Locus.Storage.Tests
{
    public class QuotaReconciliationServiceTests
    {
        [Fact]
        public async Task ExecuteAsync_RunsQuotaReconciliation_WhenEnabled()
        {
            var cleanupService = new Mock<IStorageCleanupService>(MockBehavior.Strict);
            var logger = new Mock<ILogger<QuotaReconciliationService>>();
            using var cts = new CancellationTokenSource();

            cleanupService
                .Setup(s => s.ReconcileAllQuotaCountsAsync(It.IsAny<CancellationToken>()))
                .Returns<CancellationToken>(ct =>
                {
                    cts.Cancel();
                    return Task.CompletedTask;
                });

            var service = new TestQuotaReconciliationService(
                cleanupService.Object,
                new QuotaReconciliationOptions
                {
                    RunOnStartup = true,
                    InitialDelay = TimeSpan.Zero,
                    Interval = TimeSpan.FromHours(6)
                },
                logger.Object);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => service.RunAsync(cts.Token));

            cleanupService.Verify(s => s.ReconcileAllQuotaCountsAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        private sealed class TestQuotaReconciliationService : QuotaReconciliationService
        {
            public TestQuotaReconciliationService(
                IStorageCleanupService cleanupService,
                QuotaReconciliationOptions options,
                ILogger<QuotaReconciliationService> logger)
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
