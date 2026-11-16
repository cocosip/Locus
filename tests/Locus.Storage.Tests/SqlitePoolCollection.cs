using Xunit;

namespace Locus.Storage.Tests
{
    /// <summary>
    /// Serializes tests that call SqliteConnection.ClearAllPools(), which clears the process-wide
    /// SQLite pool and can close connections other tests are still using.
    /// </summary>
    [CollectionDefinition("sqlite-pool", DisableParallelization = true)]
    public sealed class SqlitePoolCollection
    {
    }
}
