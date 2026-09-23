using System;
using Locus.Core.Models;
using Xunit;

namespace Locus.Storage.Tests
{
    public sealed class SourceCleanupOptionsTests
    {
        [Fact]
        public void Validate_DefaultOptions_AreAccepted()
        {
            new SourceCleanupOptions().Validate();
        }

        [Theory]
        [InlineData(nameof(SourceCleanupOptions.PollingInterval))]
        [InlineData(nameof(SourceCleanupOptions.MaxConcurrentActions))]
        [InlineData(nameof(SourceCleanupOptions.MaxActiveJobs))]
        [InlineData(nameof(SourceCleanupOptions.TerminalJobRetentionPeriod))]
        [InlineData(nameof(SourceCleanupOptions.ImportReservationTimeout))]
        [InlineData(nameof(SourceCleanupOptions.DatabaseOptimizationInterval))]
        [InlineData(nameof(SourceCleanupOptions.TerminalPruneBatchSize))]
        public void Validate_NonPositiveBoundedOption_Throws(string propertyName)
        {
            var options = new SourceCleanupOptions();
            switch (propertyName)
            {
                case nameof(SourceCleanupOptions.PollingInterval):
                    options.PollingInterval = TimeSpan.Zero;
                    break;
                case nameof(SourceCleanupOptions.MaxConcurrentActions):
                    options.MaxConcurrentActions = 0;
                    break;
                case nameof(SourceCleanupOptions.MaxActiveJobs):
                    options.MaxActiveJobs = 0;
                    break;
                case nameof(SourceCleanupOptions.TerminalJobRetentionPeriod):
                    options.TerminalJobRetentionPeriod = TimeSpan.Zero;
                    break;
                case nameof(SourceCleanupOptions.ImportReservationTimeout):
                    options.ImportReservationTimeout = TimeSpan.Zero;
                    break;
                case nameof(SourceCleanupOptions.DatabaseOptimizationInterval):
                    options.DatabaseOptimizationInterval = TimeSpan.Zero;
                    break;
                case nameof(SourceCleanupOptions.TerminalPruneBatchSize):
                    options.TerminalPruneBatchSize = 0;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(propertyName));
            }

            var exception = Assert.Throws<InvalidOperationException>(() => options.Validate());
            Assert.Contains(propertyName, exception.Message);
        }

        [Fact]
        public void Validate_EnabledWithEmptyDatabasePath_Throws()
        {
            var options = new SourceCleanupOptions
            {
                Enabled = true,
                DatabasePath = " "
            };

            var exception = Assert.Throws<InvalidOperationException>(() => options.Validate());
            Assert.Contains(nameof(SourceCleanupOptions.DatabasePath), exception.Message);
        }
    }
}
