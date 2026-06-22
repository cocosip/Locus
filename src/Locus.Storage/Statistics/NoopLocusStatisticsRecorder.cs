using System;
using System.Collections.Generic;
using Locus.Core.Abstractions;
using Locus.Core.Models;

namespace Locus.Storage.Statistics
{
    /// <summary>
    /// No-op statistics recorder used when statistics are disabled.
    /// </summary>
    public sealed class NoopLocusStatisticsRecorder : ILocusStatisticsRecorder, ILocusStatisticsReader
    {
        /// <summary>
        /// Gets the singleton no-op recorder instance.
        /// </summary>
        public static readonly NoopLocusStatisticsRecorder Instance = new NoopLocusStatisticsRecorder();

        private NoopLocusStatisticsRecorder()
        {
        }

        /// <inheritdoc/>
        public void Record(
            string name,
            long value,
            DateTimeOffset timestamp,
            IReadOnlyDictionary<string, string?>? dimensions = null)
        {
        }

        /// <inheritdoc/>
        public LocusStatisticsSnapshot GetSnapshot(LocusStatisticsQuery query)
        {
            return new LocusStatisticsSnapshot();
        }
    }
}
