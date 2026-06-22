using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Reads in-process Locus statistics snapshots.
    /// </summary>
    public interface ILocusStatisticsReader
    {
        /// <summary>
        /// Returns an aggregate snapshot for the requested time range and dimensions.
        /// </summary>
        LocusStatisticsSnapshot GetSnapshot(LocusStatisticsQuery query);
    }
}
