using System.Threading;
using System.Threading.Tasks;

namespace Locus.Storage
{
    /// <summary>
    /// Represents maintenance and recovery operations over projected metadata storage.
    /// </summary>
    public interface IMetadataProjectionMaintenanceStore
    {
        /// <summary>
        /// Begins an exclusive metadata database rebuild for a tenant.
        /// </summary>
        Task<IDatabaseRebuildLockHandle> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct = default);

        /// <summary>
        /// Optimizes a tenant metadata database and returns size statistics.
        /// </summary>
        Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct = default);
    }
}
