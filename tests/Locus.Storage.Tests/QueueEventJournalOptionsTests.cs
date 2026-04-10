using System;
using Locus.Storage;
using Xunit;

namespace Locus.Storage.Tests
{
    public class QueueEventJournalOptionsTests
    {
        [Fact]
        public void Validate_DisabledJournalWithoutCompatibilityFlag_Throws()
        {
            var options = new QueueEventJournalOptions
            {
                Enabled = false,
                AllowLegacyNonJournalMode = false,
                EnableProjection = false
            };

            var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
            Assert.Contains("AllowLegacyNonJournalMode", ex.Message);
        }

        [Fact]
        public void Validate_DisabledJournalWithCompatibilityFlag_AllowsLegacyMode()
        {
            var options = new QueueEventJournalOptions
            {
                Enabled = false,
                AllowLegacyNonJournalMode = true,
                EnableProjection = false
            };

            options.Validate();
        }

        [Fact]
        public void Validate_DisabledJournalCannotEnableProjection()
        {
            var options = new QueueEventJournalOptions
            {
                Enabled = false,
                AllowLegacyNonJournalMode = true,
                EnableProjection = true
            };

            var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
            Assert.Contains("EnableProjection", ex.Message);
        }

        [Fact]
        public void Validate_NegativeStateFlushDebounce_Throws()
        {
            var options = new QueueEventJournalOptions
            {
                StateFlushDebounce = TimeSpan.FromMilliseconds(-1)
            };

            var ex = Assert.Throws<InvalidOperationException>(() => options.Validate());
            Assert.Contains("StateFlushDebounce", ex.Message);
        }
    }
}
