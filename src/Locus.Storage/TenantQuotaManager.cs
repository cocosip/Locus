using System;
using System.Collections.Concurrent;
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
        private readonly ConcurrentDictionary<string, SemaphoreSlim> _globalQuotaLocks;

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaManager"/> class.
        /// </summary>
        public TenantQuotaManager(
            DirectoryQuotaRepository repository,
            ILogger<TenantQuotaManager> logger)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _globalQuotaLocks = new ConcurrentDictionary<string, SemaphoreSlim>();
        }

        /// <inheritdoc/>
        public async Task<bool> CanAddFileAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var effectiveLimit = await GetEffectiveLimitAsync(tenantId, ct);

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

            var tenantQuota = await _repository.GetAsync(tenantId, tenantId, ct);
            if (tenantQuota != null && tenantQuota.MaxCount > 0)
            {
                var success = await _repository.TryIncrementAsync(tenantId, tenantId, ct);
                if (!success)
                {
                    var currentCount = await GetFileCountAsync(tenantId, ct);
                    throw new TenantQuotaExceededException(tenantId, currentCount, tenantQuota.MaxCount);
                }

                _logger.LogDebug("Incremented file count for tenant {TenantId} (limit: {EffectiveLimit})",
                    tenantId, tenantQuota.MaxCount);
                return;
            }

            var globalLimit = await GetGlobalLimitAsync(ct);
            if (globalLimit == 0)
            {
                // No limit configured — use ForceIncrementAsync so the counter always advances
                // regardless of any MaxCount value that might exist on the quota record.
                await _repository.ForceIncrementAsync(tenantId, tenantId, ct);
                _logger.LogDebug("Incremented file count for tenant {TenantId} (limit: unlimited)", tenantId);
                return;
            }

            // Global quota is shared configuration. Keep it out of tenant-specific quota records
            // so later global limit changes still affect tenants that do not have explicit overrides.
            var gate = _globalQuotaLocks.GetOrAdd(tenantId, _ => new SemaphoreSlim(1, 1));
            await gate.WaitAsync(ct);
            try
            {
                var currentCount = await GetFileCountAsync(tenantId, ct);
                if (currentCount >= globalLimit)
                    throw new TenantQuotaExceededException(tenantId, currentCount, globalLimit);

                await _repository.TryIncrementAsync(tenantId, tenantId, ct);
            }
            finally
            {
                gate.Release();
            }

            _logger.LogDebug("Incremented file count for tenant {TenantId} (limit: {EffectiveLimit})",
                tenantId, globalLimit);
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

            var quota = await _repository.GetOrCreateAsync(tenantId, tenantId, ct);
            return quota.CurrentCount;
        }

        /// <inheritdoc/>
        public async Task<int> GetEffectiveLimitAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            var tenantQuota = await _repository.GetAsync(tenantId, tenantId, ct);
            if (tenantQuota != null && tenantQuota.MaxCount > 0)
            {
                _logger.LogDebug("Using tenant-specific quota for {TenantId}: {MaxCount}", tenantId, tenantQuota.MaxCount);
                return tenantQuota.MaxCount;
            }

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

            var snapshot = await _repository.GetOrCreateAsync(tenantId, tenantId, ct);

            // Build a fresh object instead of mutating the returned reference so we never
            // accidentally overwrite the live CurrentCount with a stale or zero value.
            var updated = new DirectoryQuota
            {
                DirectoryPath = snapshot.DirectoryPath,
                CurrentCount = snapshot.CurrentCount,
                MaxCount = maxFiles,
                Enabled = true,
                CreatedAt = snapshot.CreatedAt,
                LastUpdated = snapshot.LastUpdated
            };

            await _repository.UpdateAsync(tenantId, updated, ct);

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
                quota.MaxCount = 0;
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

            // Build a fresh object to avoid mutating the cached reference.
            // Do NOT overwrite CurrentCount — it tracks real file counts across restarts.
            var updated = new DirectoryQuota
            {
                DirectoryPath = globalQuota.DirectoryPath,
                CurrentCount = globalQuota.CurrentCount,
                MaxCount = maxFiles,
                Enabled = true,
                CreatedAt = globalQuota.CreatedAt,
                LastUpdated = globalQuota.LastUpdated
            };

            await _repository.UpdateAsync(GLOBAL_QUOTA_KEY, updated, ct);

            _logger.LogInformation("Set global quota limit: {MaxFiles} files (0 = unlimited)", maxFiles);
        }

        /// <inheritdoc/>
        public async Task<int> GetGlobalLimitAsync(CancellationToken ct)
        {
            var globalQuota = await _repository.GetAsync(GLOBAL_QUOTA_KEY, GLOBAL_QUOTA_KEY, ct);
            return globalQuota?.MaxCount ?? 0;
        }

        internal async Task CompensateIncrementFileCountAsync(string tenantId, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            // Use ForceIncrementAsync (unconditional atomic increment) rather than
            // GetOrCreateAsync + SetCurrentCountAsync to avoid the TOCTOU race where a
            // concurrent writer could read the same stale CurrentCount and both add 1,
            // or a concurrent decrement could be lost.
            await _repository.ForceIncrementAsync(tenantId, tenantId, ct);

            _logger.LogDebug("Compensated file count for tenant {TenantId}", tenantId);
        }
    }
}
