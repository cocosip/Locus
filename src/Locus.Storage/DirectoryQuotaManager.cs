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
    /// Manages logical-directory quota reservations and projected counts.
    /// </summary>
    public class DirectoryQuotaManager : IDirectoryQuotaManager, IDirectoryQuotaCompensationManager, IDirectoryQuotaProjectionManager, IDirectoryQuotaReconciliationManager
    {
        private readonly IQuotaProjectionStore _quotaStore;
        private readonly ILogger<DirectoryQuotaManager> _logger;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuotaState>> _states;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaManager"/> class.
        /// </summary>
        public DirectoryQuotaManager(
            IQuotaProjectionStore quotaStore,
            ILogger<DirectoryQuotaManager> logger)
        {
            _quotaStore = quotaStore ?? throw new ArgumentNullException(nameof(quotaStore));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _states = new ConcurrentDictionary<string, ConcurrentDictionary<string, DirectoryQuotaState>>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaManager"/> class.
        /// </summary>
        public DirectoryQuotaManager(
            DirectoryQuotaRepository repository,
            ILogger<DirectoryQuotaManager> logger)
            : this(
                new DirectoryQuotaRepositoryProjectionStore(repository ?? throw new ArgumentNullException(nameof(repository))),
                logger)
        {
        }

        /// <inheritdoc/>
        public async Task<bool> CanAddFileAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                var quota = await _quotaStore.GetOrCreateQuotaAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
                if (!quota.Enabled || quota.MaxCount == 0)
                    return true;

                return state.ProjectedCount + state.ReservationCount < quota.MaxCount;
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task IncrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                var quota = await _quotaStore.GetOrCreateQuotaAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
                var currentTotal = state.ProjectedCount + state.ReservationCount;
                if (quota.Enabled && quota.MaxCount > 0 && currentTotal >= quota.MaxCount)
                {
                    throw new DirectoryQuotaExceededException(
                        normalizedDirectoryPath,
                        currentTotal,
                        quota.MaxCount);
                }

                state.ReservationCount++;
                _logger.LogDebug(
                    "Reserved directory quota for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}, limit={Limit}",
                    normalizedDirectoryPath,
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount,
                    quota.MaxCount);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        /// <inheritdoc/>
        public async Task DecrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                if (state.ReservationCount > 0)
                {
                    state.ReservationCount--;
                    _logger.LogDebug(
                        "Released directory quota reservation for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                        normalizedDirectoryPath,
                        tenantId,
                        state.ProjectedCount,
                        state.ReservationCount);
                    return;
                }

                if (state.ProjectedCount == 0)
                {
                    _logger.LogDebug(
                        "Directory quota decrement ignored because projected count is already zero: {DirectoryPath}, Tenant={TenantId}",
                        normalizedDirectoryPath,
                        tenantId);
                    return;
                }

                state.ProjectedCount--;
                await PersistProjectedCountAsync(tenantId, normalizedDirectoryPath, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Decremented projected directory quota for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    normalizedDirectoryPath,
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
        public async Task<int> GetFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
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
        public async Task<int> GetLimitAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var quota = await _quotaStore.GetOrCreateQuotaAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            return quota.MaxCount;
        }

        /// <inheritdoc/>
        public async Task SetLimitAsync(string tenantId, string directoryPath, int maxFiles, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            if (maxFiles < 0)
                throw new ArgumentException("Max files cannot be negative", nameof(maxFiles));

            var snapshot = await _quotaStore.GetOrCreateQuotaAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            var updated = new DirectoryQuota
            {
                DirectoryPath = normalizedDirectoryPath,
                CurrentCount = snapshot.CurrentCount,
                MaxCount = maxFiles,
                Enabled = maxFiles > 0,
                CreatedAt = snapshot.CreatedAt,
                LastUpdated = snapshot.LastUpdated
            };

            await _quotaStore.UpdateQuotaAsync(tenantId, updated, ct).ConfigureAwait(false);
            _logger.LogInformation("Set limit for directory {DirectoryPath}, Tenant={TenantId}: {MaxFiles}", normalizedDirectoryPath, tenantId, maxFiles);
        }

        /// <inheritdoc/>
        public async Task CompensateIncrementFileCountAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            _ = await ApplyAcceptedProjectionAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task<bool> ApplyAcceptedProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                var consumedReservation = false;
                if (state.ReservationCount > 0)
                {
                    state.ReservationCount--;
                    consumedReservation = true;
                }

                state.ProjectedCount++;
                await PersistProjectedCountAsync(tenantId, normalizedDirectoryPath, state.ProjectedCount, ct).ConfigureAwait(false);

                _logger.LogDebug(
                    "Applied Accepted directory projection for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}, consumedReservation={ConsumedReservation}",
                    normalizedDirectoryPath,
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
        public async Task RollbackAcceptedProjectionAsync(string tenantId, string directoryPath, bool restoreReservation, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                if (state.ProjectedCount > 0)
                {
                    state.ProjectedCount--;
                    await PersistProjectedCountAsync(tenantId, normalizedDirectoryPath, state.ProjectedCount, ct).ConfigureAwait(false);
                }

                if (restoreReservation)
                    state.ReservationCount++;

                _logger.LogDebug(
                    "Rolled back Accepted directory projection for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}, restoredReservation={RestoreReservation}",
                    normalizedDirectoryPath,
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
        public async Task ApplyDeleteSucceededProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                if (state.ProjectedCount == 0)
                {
                    _logger.LogDebug(
                        "DeleteSucceeded directory projection ignored because projected count is already zero: {DirectoryPath}, Tenant={TenantId}",
                        normalizedDirectoryPath,
                        tenantId);
                    return;
                }

                state.ProjectedCount--;
                await PersistProjectedCountAsync(tenantId, normalizedDirectoryPath, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Applied DeleteSucceeded directory projection for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    normalizedDirectoryPath,
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
        public async Task RollbackDeleteSucceededProjectionAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                state.ProjectedCount++;
                await PersistProjectedCountAsync(tenantId, normalizedDirectoryPath, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Rolled back DeleteSucceeded directory projection for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    normalizedDirectoryPath,
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
        public async Task SetFileCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default)
        {
            ValidateInputs(tenantId, directoryPath, out var normalizedDirectoryPath);

            if (count < 0)
                throw new ArgumentException("Count cannot be negative", nameof(count));

            var state = await EnterStateAsync(tenantId, normalizedDirectoryPath, ct).ConfigureAwait(false);
            try
            {
                state.ProjectedCount = count;
                await PersistProjectedCountAsync(tenantId, normalizedDirectoryPath, state.ProjectedCount, ct).ConfigureAwait(false);
                _logger.LogDebug(
                    "Reconciled directory quota for {DirectoryPath}, Tenant={TenantId}: projected={ProjectedCount}, reservations={ReservationCount}",
                    normalizedDirectoryPath,
                    tenantId,
                    state.ProjectedCount,
                    state.ReservationCount);
            }
            finally
            {
                state.Gate.Release();
            }
        }

        private async Task PersistProjectedCountAsync(string tenantId, string directoryPath, int projectedCount, CancellationToken ct)
        {
            await _quotaStore.SetProjectedCountAsync(tenantId, directoryPath, projectedCount, ct).ConfigureAwait(false);
        }

        private async Task<DirectoryQuotaState> EnterStateAsync(string tenantId, string directoryPath, CancellationToken ct)
        {
            var tenantStates = _states.GetOrAdd(
                tenantId,
                _ => new ConcurrentDictionary<string, DirectoryQuotaState>(StringComparer.Ordinal));
            var state = tenantStates.GetOrAdd(directoryPath, _ => new DirectoryQuotaState());
            await state.Gate.WaitAsync(ct).ConfigureAwait(false);

            try
            {
                if (!state.Initialized)
                {
                    var snapshot = await _quotaStore.GetOrCreateQuotaAsync(tenantId, directoryPath, ct).ConfigureAwait(false);
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

        private static void ValidateInputs(string tenantId, string directoryPath, out string normalizedDirectoryPath)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("Tenant ID cannot be empty", nameof(tenantId));

            normalizedDirectoryPath = DirectoryPathNormalizer.Normalize(directoryPath);
        }

        private sealed class DirectoryQuotaState
        {
            public SemaphoreSlim Gate { get; } = new SemaphoreSlim(1, 1);

            public int ProjectedCount { get; set; }

            public int ReservationCount { get; set; }

            public bool Initialized { get; set; }
        }
    }
}
