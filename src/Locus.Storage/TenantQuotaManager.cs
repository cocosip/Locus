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
    /// Manages file count quotas for tenants.
    /// Supports global default quota and per-tenant specific quotas.
    /// Uses DirectoryQuotaRepository internally (treating tenantId as directoryPath).
    /// </summary>
    public class TenantQuotaManager : ITenantQuotaManager
    {
        private const string GLOBAL_QUOTA_KEY = "_GLOBAL_QUOTA_";
        private readonly DirectoryQuotaRepository _repository;
        private readonly ILogger<TenantQuotaManager> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaManager"/> class.
        /// </summary>
        public TenantQuotaManager(
            DirectoryQuotaRepository repository,
            ILogger<TenantQuotaManager> logger)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc/>
        public async Task<bool> CanAddFileAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var effectiveLimit = await GetEffectiveLimitAsync(tenantId, ct);

            // 0 means unlimited
            if (effectiveLimit == 0)
                return true;

            var currentCount = await GetFileCountAsync(tenantId, ct);
            return currentCount < effectiveLimit;
        }

        /// <inheritdoc/>
        public async Task IncrementFileCountAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // Check quota before incrementing
            var canAdd = await CanAddFileAsync(tenantId, ct);
            if (!canAdd)
            {
                var currentCount = await GetFileCountAsync(tenantId, ct);
                var limit = await GetEffectiveLimitAsync(tenantId, ct);
                throw new TenantQuotaExceededException(tenantId, currentCount, limit);
            }

            // Increment count
            var quota = await _repository.GetOrCreateAsync(tenantId, tenantId, ct);
            quota.CurrentCount++;
            quota.LastUpdated = DateTime.UtcNow;
            await _repository.UpdateAsync(tenantId, quota, ct);

            _logger.LogDebug("Incremented file count for tenant {TenantId}: {CurrentCount}/{EffectiveLimit}",
                tenantId, quota.CurrentCount, await GetEffectiveLimitAsync(tenantId, ct));
        }

        /// <inheritdoc/>
        public async Task DecrementFileCountAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            await _repository.DecrementAsync(tenantId, tenantId, ct);

            _logger.LogDebug("Decremented file count for tenant {TenantId}", tenantId);
        }

        /// <inheritdoc/>
        public async Task<int> GetFileCountAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var quota = await _repository.GetAsync(tenantId, tenantId, ct);
            return quota?.CurrentCount ?? 0;
        }

        /// <inheritdoc/>
        public async Task<int> GetEffectiveLimitAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // Try to get tenant-specific quota
            var tenantQuota = await _repository.GetAsync(tenantId, tenantId, ct);
            if (tenantQuota != null && tenantQuota.MaxCount > 0)
            {
                _logger.LogDebug("Using tenant-specific quota for {TenantId}: {MaxCount}", tenantId, tenantQuota.MaxCount);
                return tenantQuota.MaxCount;
            }

            // Fall back to global quota
            var globalLimit = await GetGlobalLimitAsync(ct);
            _logger.LogDebug("Using global quota for {TenantId}: {MaxCount}", tenantId, globalLimit);
            return globalLimit;
        }

        /// <inheritdoc/>
        public async Task SetTenantLimitAsync(string tenantId, int maxFiles, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (maxFiles < 0)
                throw new ArgumentException("Max files cannot be negative", nameof(maxFiles));

            var quota = await _repository.GetOrCreateAsync(tenantId, tenantId, ct);
            quota.MaxCount = maxFiles;
            quota.Enabled = true;

            await _repository.UpdateAsync(tenantId, quota, ct);

            _logger.LogInformation("Set tenant-specific quota for {TenantId}: {MaxFiles} files", tenantId, maxFiles);
        }

        /// <inheritdoc/>
        public async Task RemoveTenantLimitAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var quota = await _repository.GetAsync(tenantId, tenantId, ct);
            if (quota != null)
            {
                quota.MaxCount = 0;  // 0 means use global quota
                await _repository.UpdateAsync(tenantId, quota, ct);
                _logger.LogInformation("Removed tenant-specific quota for {TenantId}, will use global quota", tenantId);
            }
        }

        /// <inheritdoc/>
        public async Task SetGlobalLimitAsync(int maxFiles, CancellationToken ct)
        {
            if (maxFiles < 0)
                throw new ArgumentException("Max files cannot be negative", nameof(maxFiles));

            var globalQuota = await _repository.GetOrCreateAsync(GLOBAL_QUOTA_KEY, GLOBAL_QUOTA_KEY, ct);
            globalQuota.MaxCount = maxFiles;
            globalQuota.Enabled = true;
            globalQuota.CurrentCount = 0;  // Global quota doesn't track count

            await _repository.UpdateAsync(GLOBAL_QUOTA_KEY, globalQuota, ct);

            _logger.LogInformation("Set global quota limit: {MaxFiles} files (0 = unlimited)", maxFiles);
        }

        /// <inheritdoc/>
        public async Task<int> GetGlobalLimitAsync(CancellationToken ct)
        {
            var globalQuota = await _repository.GetAsync(GLOBAL_QUOTA_KEY, GLOBAL_QUOTA_KEY, ct);
            return globalQuota?.MaxCount ?? 0;  // 0 means unlimited
        }
    }
}

