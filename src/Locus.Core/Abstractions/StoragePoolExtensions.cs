using System;
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
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload without specifying a tenant (uses default tenant).
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            byte[] content,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, null, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload without specifying a tenant (uses default tenant) but with original file name.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            byte[] content,
            string? originalFileName,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, originalFileName, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload without specifying a tenant (uses default tenant) with original file name
        /// and logical directory path.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            byte[] content,
            string? originalFileName,
            string? logicalDirectoryPath,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(
                CreateDefaultTenantContext(),
                content,
                originalFileName,
                logicalDirectoryPath,
                ct);
        }

        /// <summary>
        /// Writes an in-memory payload without specifying a tenant (uses default tenant).
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ArraySegment<byte> content,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, null, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload without specifying a tenant (uses default tenant) but with original file name.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ArraySegment<byte> content,
            string? originalFileName,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, originalFileName, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload without specifying a tenant (uses default tenant) with original file name
        /// and logical directory path.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ArraySegment<byte> content,
            string? originalFileName,
            string? logicalDirectoryPath,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(
                CreateDefaultTenantContext(),
                content,
                originalFileName,
                logicalDirectoryPath,
                ct);
        }

        /// <summary>
        /// Writes a file without specifying a tenant (uses default tenant) but with original file name.
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            Stream content,
            string? originalFileName,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, originalFileName, ct);
        }

        /// <summary>
        /// Writes a file without specifying a tenant (uses default tenant) with original file name
        /// and logical directory path.
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            Stream content,
            string? originalFileName,
            string? logicalDirectoryPath,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(CreateDefaultTenantContext(), content, originalFileName, logicalDirectoryPath, ct);
        }

        /// <summary>
        /// Writes an in-memory payload for an explicit tenant.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ITenantContext tenant,
            byte[] content,
            string? originalFileName,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(tenant, content, originalFileName, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload for an explicit tenant with logical directory path.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static async Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ITenantContext tenant,
            byte[] content,
            string? originalFileName,
            string? logicalDirectoryPath,
            CancellationToken ct = default)
        {
            if (storagePool == null)
                throw new System.ArgumentNullException(nameof(storagePool));

            if (tenant == null)
                throw new System.ArgumentNullException(nameof(tenant));

            if (content == null)
                throw new System.ArgumentNullException(nameof(content));

            using (var stream = CreateVisibleMemoryStream(new ArraySegment<byte>(content)))
            {
                return await storagePool.WriteFileAsync(tenant, stream, originalFileName, logicalDirectoryPath, ct)
                    .ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Writes an in-memory payload for an explicit tenant.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ITenantContext tenant,
            ArraySegment<byte> content,
            string? originalFileName,
            CancellationToken ct = default)
        {
            return storagePool.WriteFileAsync(tenant, content, originalFileName, null, ct);
        }

        /// <summary>
        /// Writes an in-memory payload for an explicit tenant with logical directory path.
        /// Uses a publicly visible MemoryStream wrapper so the storage volume can take the direct-memory fast path.
        /// </summary>
        public static async Task<string> WriteFileAsync(
            this IStoragePool storagePool,
            ITenantContext tenant,
            ArraySegment<byte> content,
            string? originalFileName,
            string? logicalDirectoryPath,
            CancellationToken ct = default)
        {
            if (storagePool == null)
                throw new System.ArgumentNullException(nameof(storagePool));

            if (tenant == null)
                throw new System.ArgumentNullException(nameof(tenant));

            using (var stream = CreateVisibleMemoryStream(content))
            {
                return await storagePool.WriteFileAsync(tenant, stream, originalFileName, logicalDirectoryPath, ct)
                    .ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Reads a file without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<Stream> ReadFileAsync(
            this IStoragePool storagePool,
            string fileKey,
            CancellationToken ct = default)
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
            CancellationToken ct = default)
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
            CancellationToken ct = default)
        {
            return storagePool.GetFileLocationAsync(CreateDefaultTenantContext(), fileKey, ct);
        }

        /// <summary>
        /// Gets next file for processing without specifying a tenant (uses default tenant).
        /// For single-tenant scenarios only.
        /// </summary>
        public static Task<FileLocation?> GetNextFileForProcessingAsync(
            this IStoragePool storagePool,
            CancellationToken ct = default)
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
            CancellationToken ct = default)
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

        private static MemoryStream CreateVisibleMemoryStream(ArraySegment<byte> content)
        {
            var buffer = content.Array;
            if (buffer == null)
            {
                if (content.Count == 0 && content.Offset == 0)
                    buffer = System.Array.Empty<byte>();
                else
                    throw new System.ArgumentException("ArraySegment must reference an underlying byte array.", nameof(content));
            }

            return new MemoryStream(
                buffer,
                content.Offset,
                content.Count,
                writable: false,
                publiclyVisible: true);
        }
    }
}
