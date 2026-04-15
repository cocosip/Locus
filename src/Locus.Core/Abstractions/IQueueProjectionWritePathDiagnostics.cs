using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Exposes point-in-time diagnostics for projection-store enqueue work.
    /// </summary>
    public interface IQueueProjectionWritePathDiagnostics
    {
        /// <summary>
        /// Returns a point-in-time snapshot of projection write-path counters.
        /// </summary>
        QueueProjectionWritePathStatistics GetWritePathStatisticsSnapshot();
    }
}
