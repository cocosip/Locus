using Xunit;

namespace Locus.Storage.Tests
{
    /// <summary>
    /// Serializes tests that observe the process-wide queue journal meter so parallel
    /// tests cannot add measurements to the same capture window.
    /// </summary>
    [CollectionDefinition("QueueJournalMetrics", DisableParallelization = true)]
    public sealed class QueueJournalMetricsCollection
    {
    }
}
