using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Exposes aggregated diagnostics for storage-volume write-path observations.
    /// </summary>
    public interface IStorageVolumeWritePathDiagnostics
    {
        /// <summary>
        /// Returns a point-in-time snapshot of write-path observation counters.
        /// </summary>
        StorageVolumeWritePathStatistics GetWritePathStatisticsSnapshot();
    }
}
