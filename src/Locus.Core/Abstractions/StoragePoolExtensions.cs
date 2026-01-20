using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;

namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Extension methods for IStoragePool to simplify single-tenant scenarios.
    /// </summary>
    public static class StoragePoolExtensions
    {
        private const string DefaultTenantId = "default";

        /// <summary>
        /// Creates a default tenant context for single-tenant scenarios.
        /// </summary>
        public static ITenantContext CreateDefaultTenantContext()
        {
            return new DefaultTenantContext(DefaultTenantId);
        }

        /// <summary>
        /// Writes a file without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            Stream content,
            CancellationToken ct)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, null, ct);
        }

        /// <summary>
        /// Writes a file without specifying a tenant (uses default tenant) but with original file name.
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            Stream content,
            string? originalFileName,
            CancellationToken ct)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, originalFileName, ct);
        }

        /// <summary>
        /// Reads a file without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<Stream> ReadFileAsync(
            this IStoragePool storagePool,
            string fileKey,
            CancellationToken ct)
        {
            return storagePool.ReadFileAsync(CreateDefaultTenantContext(), fileKey, ct);
        }

        /// <summary>
        /// Gets file info without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<Models.FileInfo?> GetFileInfoAsync(
            this IStoragePool storagePool,
            string fileKey,
            CancellationToken ct)
        {
            return storagePool.GetFileInfoAsync(CreateDefaultTenantContext(), fileKey, ct);
        }

        /// <summary>
        /// Gets file location without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<FileLocation?> GetFileLocationAsync(
            this IStoragePool storagePool,
            string fileKey,
            CancellationToken ct)
        {
            return storagePool.GetFileLocationAsync(CreateDefaultTenantContext(), fileKey, ct);
        }

        /// <summary>
        /// Gets next file for processing without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<FileLocation?> GetNextFileForProcessingAsync(
            this IStoragePool storagePool,
            CancellationToken ct)
        {
            return storagePool.GetNextFileForProcessingAsync(CreateDefaultTenantContext(), ct);
        }

        /// <summary>
        /// Gets next batch for processing without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(
            this IStoragePool storagePool,
            int batchSize,
            CancellationToken ct)
        {
            return storagePool.GetNextBatchForProcessingAsync(CreateDefaultTenantContext(), batchSize, ct);
        }

        /// <summary>
        /// Default tenant context implementation for single-tenant scenarios.
        /// </summary>
        private class DefaultTenantContext : ITenantContext
        {
            public DefaultTenantContext(string tenantId)
            {
                TenantId = tenantId;
                Status = TenantStatus.Enabled;
            }

            public string TenantId { get; }
            public TenantStatus Status { get; }
        }
    }
}
