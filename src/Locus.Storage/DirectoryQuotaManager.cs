using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Exceptions;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// Manages directory-level file count quotas with thread-safe operations.
    /// </summary>
    public class DirectoryQuotaManager : IDirectoryQuotaManager
    {
        private readonly DirectoryQuotaRepository _repository;
        private readonly ILogger<DirectoryQuotaManager> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaManager"/> class.
        /// </summary>
        public DirectoryQuotaManager(
            DirectoryQuotaRepository repository,
            ILogger<DirectoryQuotaManager> logger)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async Task<bool> CanAddFileAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var quota = await _repository.GetOrCreateAsync(tenantId, directoryPath, ct);

            // If quota is disabled or no limit set, allow
            if (!quota.Enabled || quota.MaxCount == 0)
            {
                return true;
            }

            // Check if adding would exceed limit
            return quota.CurrentCount < quota.MaxCount;
        }

        /// <inheritdoc/>
        public async Task IncrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var success = await _repository.TryIncrementAsync(tenantId, directoryPath, ct);

            if (!success)
            {
                var quota = await _repository.GetAsync(tenantId, directoryPath, ct);
                throw new DirectoryQuotaExceededException(
                    directoryPath,
                    quota?.CurrentCount ?? 0,
                    quota?.MaxCount ?? 0);
            }

            _logger.LogDebug("Incremented file count for directory: {DirectoryPath}, Tenant: {TenantId}", directoryPath, tenantId);
        }

        /// <inheritdoc/>
        public async Task DecrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            await _repository.DecrementAsync(tenantId, directoryPath, ct);

            _logger.LogDebug("Decremented file count for directory: {DirectoryPath}, Tenant: {TenantId}", directoryPath, tenantId);
        }

        /// <inheritdoc/>
        public async Task<int> GetFileCountAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var quota = await _repository.GetOrCreateAsync(tenantId, directoryPath, ct);
            return quota.CurrentCount;
        }

        /// <inheritdoc/>
        public async Task<int> GetLimitAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            var quota = await _repository.GetOrCreateAsync(tenantId, directoryPath, ct);
            return quota.MaxCount;
        }

        /// <inheritdoc/>
        public async Task SetLimitAsync(string tenantId, string directoryPath, int maxFiles, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            if (string.IsNullOrWhiteSpace(directoryPath))
                throw new ArgumentException("Directory path cannot be empty", nameof(directoryPath));

            if (maxFiles < 0)
                throw new ArgumentException("Max files cannot be negative", nameof(maxFiles));

            var quota = await _repository.GetOrCreateAsync(tenantId, directoryPath, ct);
            quota.MaxCount = maxFiles;
            quota.Enabled = maxFiles > 0; // Auto-enable if limit > 0, disable if limit = 0

            await _repository.UpdateAsync(tenantId, quota, ct);

            _logger.LogInformation("Set limit for directory {DirectoryPath}, Tenant: {TenantId}: {MaxFiles}",
                directoryPath, tenantId, maxFiles);
        }
    }
}
