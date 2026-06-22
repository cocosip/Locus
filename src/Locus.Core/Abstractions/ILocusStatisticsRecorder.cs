using System;
using System.Collections.Generic;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Records low-overhead in-process Locus statistics.
    /// </summary>
    public interface ILocusStatisticsRecorder
    {
        /// <summary>
        /// Records an aggregate delta.
        /// </summary>
        void Record(
            string name,
            long value,
            DateTimeOffset timestamp,
            IReadOnlyDictionary<string, string?>? dimensions = null);
    }
}
