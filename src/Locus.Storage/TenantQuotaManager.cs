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
    /// Manages tenant-level quota reservations and projected counts.
    /// </summary>
    public class TenantQuotaManager : ITenantQuotaManager, ITenantQuotaCompensationManager, ITenantQuotaProjectionManager, ITenantQuotaReconciliationManager
    {
        private const string GLOBAL_QUOTA_KEY = "_GLOBAL_QUOTA_";
        private readonly IQuotaProjectionStore _quotaStore;
        private readonly ILogger<TenantQuotaManager> _logger;
        private readonly ConcurrentDictionary<string, TenantQuotaState> _states;

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaManager"/> class.
        /// </summary>
        public TenantQuotaManager(
            IQuotaProjectionStore quotaStore,
            ILogger<TenantQuotaManager> logger)
        {
            _quotaStore = quotaStore ?? throw new ArgumentNullException(nameof(quotaStore));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _states = new ConcurrentDictionary<string, TenantQuotaState>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TenantQuotaManager"/> class.
        /// </summary>
        public TenantQuotaManager(
            DirectoryQuotaRepository repository,
            ILogger<TenantQuotaManager> logger)
            : this(
                new DirectoryQuotaRepositoryProjectionStore(repository ?? throw new ArgumentNullException(nameof(repository))),
                logger)
        {
        }

        /// <inheritdoc/>
        public async Task<bool> CanAddFileAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                var effectiveLimit = await GetEffectiveLimitCoreAsync(tenantId, ct).ConfigureAwait(false);
                if (effectiveLimit == 0)
                    return true;

                return state.ProjectedCount + state.ReservationCount < effectiveLimit;
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task IncrementFileCountAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                var effectiveLimit = await GetEffectiveLimitCoreAsync(tenantId, ct).ConfigureAwait(false);
                var currentTotal = state.ProjectedCount + state.ReservationCount;
                if (effectiveLimit > 0 && currentTotal >= effectiveLimit)
                    throw new TenantQuotaExceededException(tenantId, currentTotal, effectiveLimit);

                state.ReservationCount++;
                _logger.LogDebug(
                    "Reserved tenant quota for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}, limit={EffectiveLimit}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount,
                    effectiveLimit);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task DecrementFileCountAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                if (state.ReservationCount > 0)
                {
                    state.ReservationCount--;
                    _logger.LogDebug(
                        "Released tenant quota reservation for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                        tenantId,
                        state.ProjectedCount,
                        state.ReservationCount);
                    return;
                }

                if (state.ProjectedCount == 0)
                {
                    _logger.LogDebug("Tenant quota decrement ignored because projected count is already zero: {TenantId}", tenantId);
                    return;
                }

                state.ProjectedCount--;
                await PersistProjectedCountAsync(tenantId, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Decremented projected tenant quota for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<int> GetFileCountAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                return state.ProjectedCount + state.ReservationCount;
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task<int> GetEffectiveLimitAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);
            return await GetEffectiveLimitCoreAsync(tenantId, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task SetTenantLimitAsync(string tenantId, int maxFiles, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            if (maxFiles < 0)
                throw new ArgumentException("Max files cannot be negative", nameof(maxFiles));

            var snapshot = await _quotaStore.GetOrCreateQuotaAsync(tenantId, tenantId, ct).ConfigureAwait(false);
            var updated = new DirectoryQuota
            {
                DirectoryPath = snapshot.DirectoryPath,
                CurrentCount = snapshot.CurrentCount,
                MaxCount = maxFiles,
                Enabled = maxFiles > 0,
                CreatedAt = snapshot.CreatedAt,
                LastUpdated = snapshot.LastUpdated
            };

            await _quotaStore.UpdateQuotaAsync(tenantId, updated, ct).ConfigureAwait(false);

            _logger.LogInformation("Set tenant-specific quota for {TenantId}: {MaxFiles} files", tenantId, maxFiles);
        }

        /// <inheritdoc/>
        public async Task RemoveTenantLimitAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var quota = await _quotaStore.GetQuotaAsync(tenantId, tenantId, ct).ConfigureAwait(false);
            if (quota == null)
                return;

            var updated = new DirectoryQuota
            {
                DirectoryPath = quota.DirectoryPath,
                CurrentCount = quota.CurrentCount,
                MaxCount = 0,
                Enabled = false,
                CreatedAt = quota.CreatedAt,
                LastUpdated = quota.LastUpdated
            };

            await _quotaStore.UpdateQuotaAsync(tenantId, updated, ct).ConfigureAwait(false);
            _logger.LogInformation("Removed tenant-specific quota for {TenantId}, will use global quota", tenantId);
        }

        /// <inheritdoc/>
        public async Task SetGlobalLimitAsync(int maxFiles, CancellationToken ct = default)
        {
            if (maxFiles < 0)
                throw new ArgumentException("Max files cannot be negative", nameof(maxFiles));

            var globalQuota = await _quotaStore.GetOrCreateQuotaAsync(GLOBAL_QUOTA_KEY, GLOBAL_QUOTA_KEY, ct).ConfigureAwait(false);
            var updated = new DirectoryQuota
            {
                DirectoryPath = globalQuota.DirectoryPath,
                CurrentCount = globalQuota.CurrentCount,
                MaxCount = maxFiles,
                Enabled = maxFiles > 0,
                CreatedAt = globalQuota.CreatedAt,
                LastUpdated = globalQuota.LastUpdated
            };

            await _quotaStore.UpdateQuotaAsync(GLOBAL_QUOTA_KEY, updated, ct).ConfigureAwait(false);
            _logger.LogInformation("Set global quota limit: {MaxFiles} files (0 = unlimited)", maxFiles);
        }

        /// <inheritdoc/>
        public async Task<int> GetGlobalLimitAsync(CancellationToken ct = default)
        {
            var globalQuota = await _quotaStore.GetQuotaAsync(GLOBAL_QUOTA_KEY, GLOBAL_QUOTA_KEY, ct).ConfigureAwait(false);
            return globalQuota?.MaxCount ?? 0;
        }

        /// <inheritdoc/>
        public async Task CompensateIncrementFileCountAsync(string tenantId, CancellationToken ct = default)
        {
            _ = await ApplyAcceptedProjectionAsync(tenantId, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<bool> ApplyAcceptedProjectionAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                var consumedReservation = false;
                if (state.ReservationCount > 0)
                {
                    state.ReservationCount--;
                    consumedReservation = true;
                }

                state.ProjectedCount++;
                await PersistProjectedCountAsync(tenantId, state.ProjectedCount, ct).ConfigureAwait(false);

                _logger.LogDebug(
                    "Applied Accepted tenant projection for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}, consumedReservation={ConsumedReservation}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount,
                    consumedReservation);

                return consumedReservation;
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task RollbackAcceptedProjectionAsync(string tenantId, bool restoreReservation, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                if (state.ProjectedCount > 0)
                {
                    state.ProjectedCount--;
                    await PersistProjectedCountAsync(tenantId, state.ProjectedCount, ct).ConfigureAwait(false);
                }

                if (restoreReservation)
                    state.ReservationCount++;

                _logger.LogDebug(
                    "Rolled back Accepted tenant projection for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}, restoredReservation={RestoreReservation}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount,
                    restoreReservation);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task ApplyDeleteSucceededProjectionAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                if (state.ProjectedCount == 0)
                {
                    _logger.LogDebug("DeleteSucceeded tenant projection ignored because projected count is already zero: {TenantId}", tenantId);
                    return;
                }

                state.ProjectedCount--;
                await PersistProjectedCountAsync(tenantId, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Applied DeleteSucceeded tenant projection for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task RollbackDeleteSucceededProjectionAsync(string tenantId, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                state.ProjectedCount++;
                await PersistProjectedCountAsync(tenantId, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Rolled back DeleteSucceeded tenant projection for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task SetFileCountAsync(string tenantId, int count, CancellationToken ct = default)
        {
            ValidateTenantId(tenantId);

            if (count < 0)
                throw new ArgumentException("Count cannot be negative", nameof(count));

            var state = await EnterStateAsync(tenantId, ct).ConfigureAwait(false);
            try
            {
                state.ProjectedCount = count;
                await PersistProjectedCountAsync(tenantId, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Reconciled tenant quota for {TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        private async Task<int> GetEffectiveLimitCoreAsync(string tenantId, CancellationToken ct)
        {
            var tenantQuota = await _quotaStore.GetQuotaAsync(tenantId, tenantId, ct).ConfigureAwait(false);
            if (tenantQuota != null && tenantQuota.MaxCount > 0)
            {
                _logger.LogDebug("Using tenant-specific quota for {TenantId}: {MaxCount}", tenantId, tenantQuota.MaxCount);
                return tenantQuota.MaxCount;
            }

            var globalLimit = await GetGlobalLimitAsync(ct).ConfigureAwait(false);
            _logger.LogDebug("Using global quota for {TenantId}: {MaxCount}", tenantId, globalLimit);
            return globalLimit;
        }

        private async Task PersistProjectedCountAsync(string tenantId, int projectedCount, CancellationToken ct)
        {
            await _quotaStore.SetProjectedCountAsync(tenantId, tenantId, projectedCount, ct).ConfigureAwait(false);
        }

        private async Task<TenantQuotaState> EnterStateAsync(string tenantId, CancellationToken ct)
        {
            var state = _states.GetOrAdd(tenantId, _ => new TenantQuotaState());
            await state.Gate.WaitAsync(ct).ConfigureAwait(false);

            try
            {
                if (!state.Initialized)
                {
                    var snapshot = await _quotaStore.GetOrCreateQuotaAsync(tenantId, tenantId, ct).ConfigureAwait(false);
                    state.ProjectedCount = snapshot.CurrentCount;
                    state.Initialized = true;
                }

                return state;
            }
            catch
            {
                state.Gate.Release();
                throw;
            }
        }

        private static void ValidateTenantId(string tenantId)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));
        }

        private sealed class TenantQuotaState
        {
            public SemaphoreSlim Gate { get; } = new SemaphoreSlim(1, 1);

            public int ProjectedCount { get; set; }

            public int ReservationCount { get; set; }

            public bool Initialized { get; set; }
        }
    }
}
