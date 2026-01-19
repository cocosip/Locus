using System;
using System.Collections.Concurrent;
using System.IO.Abstractions;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using Microsoft.Extensions.Logging;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Helper class for optimizing (rebuilding) LiteDB databases safely.
    /// Handles connection disposal, locking, and reconnection.
    /// </summary>
    internal static class LiteDbOptimizationHelper
    {
        /// <summary>
        /// Optimizes a LiteDB database for a specific tenant.
        /// Thread-safe: blocks all operations for this tenant during optimization.
        /// </summary>
        /// <param name="tenantId">The tenant ID.</param>
        /// <param name="dbPath">Full path to the database file.</param>
        /// <param name="databases">Dictionary of cached Lazy database connections.</param>
        /// <param name="locks">Dictionary of tenant-specific locks.</param>
        /// <param name="fileSystem">File system abstraction.</param>
        /// <param name="logger">Logger instance.</param>
        /// <param name="dbTypeName">Name of database type for logging (e.g., "metadata", "quota").</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Tuple of (size before, size after) in bytes.</returns>
        public static async Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(
            string tenantId,
            string dbPath,
            ConcurrentDictionary<string, Lazy<LiteDatabase>> databases,
            ConcurrentDictionary<string, SemaphoreSlim> locks,
            IFileSystem fileSystem,
            ILogger logger,
            string dbTypeName,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // If database doesn't exist yet, nothing to optimize
            if (!fileSystem.File.Exists(dbPath))
                return (0, 0);

            // Get or create tenant-specific lock with special prefix to avoid conflicts
            // (e.g., DirectoryQuotaRepository uses _directoryLocks for both directory and tenant operations)
            var lockKey = $"__DB_OPTIMIZATION__{tenantId}";
            var tenantLock = locks.GetOrAdd(lockKey, _ => new SemaphoreSlim(1, 1));

            // Acquire exclusive lock to block all operations for this tenant
            await tenantLock.WaitAsync(ct);
            try
            {
                long sizeBefore = fileSystem.FileInfo.New(dbPath).Length;

                logger.LogInformation("Optimizing {DbType} database for tenant {TenantId}. Current size: {SizeMB:F2} MB",
                    dbTypeName, tenantId, sizeBefore / 1024.0 / 1024.0);

                // Step 1: Dispose existing database connection
                if (databases.TryGetValue(tenantId, out var lazyDb) && lazyDb.IsValueCreated)
                {
                    try
                    {
                        lazyDb.Value?.Dispose();
                        logger.LogDebug("Disposed existing {DbType} database connection for tenant {TenantId}",
                            dbTypeName, tenantId);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "Error disposing {DbType} database connection for tenant {TenantId}",
                            dbTypeName, tenantId);
                    }
                }

                // Step 2: Remove from cache to force reconnection
                databases.TryRemove(tenantId, out _);

                // Step 3: Perform rebuild with a new connection
                await Task.Run(() =>
                {
                    var connectionString = $"Filename={dbPath};Mode=Shared";
                    using (var db = new LiteDatabase(connectionString))
                    {
                        db.Rebuild();
                        logger.LogDebug("LiteDB Rebuild completed for tenant {TenantId} {DbType} database",
                            tenantId, dbTypeName);
                    }
                }, ct);

                long sizeAfter = fileSystem.FileInfo.New(dbPath).Length;
                long spaceSaved = sizeBefore - sizeAfter;

                logger.LogInformation(
                    "Optimized {DbType} database for tenant {TenantId}. Before: {BeforeMB:F2} MB, After: {AfterMB:F2} MB, Saved: {SavedMB:F2} MB",
                    dbTypeName, tenantId, sizeBefore / 1024.0 / 1024.0, sizeAfter / 1024.0 / 1024.0, spaceSaved / 1024.0 / 1024.0);

                // Step 4: Database connection will be reloaded on next access

                return (sizeBefore, sizeAfter);
            }
            finally
            {
                tenantLock.Release();
            }
        }
    }
}
