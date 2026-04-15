using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Exposes point-in-time diagnostics for queue journal append and flush activity.
    /// </summary>
    public interface IQueueEventJournalWritePathDiagnostics
    {
        /// <summary>
        /// Returns a point-in-time snapshot of queue journal write-path counters.
        /// </summary>
        QueueJournalWritePathStatistics GetWritePathStatisticsSnapshot();
    }
}
