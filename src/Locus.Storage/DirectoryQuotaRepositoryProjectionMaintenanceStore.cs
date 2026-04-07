using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Adapts <see cref="DirectoryQuotaRepository"/> to maintenance-oriented projection operations.
    /// </summary>
    public sealed class DirectoryQuotaRepositoryProjectionMaintenanceStore : IQuotaProjectionMaintenanceStore
    {
        private readonly DirectoryQuotaRepository _repository;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaRepositoryProjectionMaintenanceStore"/> class.
        /// </summary>
        public DirectoryQuotaRepositoryProjectionMaintenanceStore(DirectoryQuotaRepository repository)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        }

        /// <inheritdoc/>
        public async Task<IDatabaseRebuildLockHandle> BeginDatabaseRebuildAsync(string tenantId, CancellationToken ct = default)
        {
            var handle = await _repository.BeginDatabaseRebuildAsync(tenantId, ct).ConfigureAwait(false);
            return new DatabaseRebuildLockHandleAdapter(handle);
        }

        /// <inheritdoc/>
        public Task ForceIncrementAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            return _repository.ForceIncrementAsync(tenantId, directoryPath, ct);
        }

        /// <inheritdoc/>
        public Task DecrementAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            return _repository.DecrementAsync(tenantId, directoryPath, ct);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<DirectoryQuota>> GetQuotaRowsAsync(string tenantId, CancellationToken ct = default)
        {
            return (await _repository.GetAllAsync(tenantId, ct).ConfigureAwait(false)).ToList();
        }

        /// <inheritdoc/>
        public Task<bool> RemoveQuotaAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            return _repository.RemoveAsync(tenantId, directoryPath, ct);
        }

        /// <inheritdoc/>
        public Task SetProjectedCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default)
        {
            return _repository.SetCurrentCountAsync(tenantId, directoryPath, count, ct);
        }

        /// <inheritdoc/>
        public Task SetProjectedCountForRebuildAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default)
        {
            return _repository.SetCurrentCountForRebuildAsync(tenantId, directoryPath, count, ct);
        }

        /// <inheritdoc/>
        public Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct = default)
        {
            return _repository.OptimizeDatabaseAsync(tenantId, ct);
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> GetTenantIdsAsync(CancellationToken ct = default)
        {
            return (await _repository.GetAllTenantIdsAsync(ct).ConfigureAwait(false)).ToList();
        }

        private sealed class DatabaseRebuildLockHandleAdapter : IDatabaseRebuildLockHandle
        {
            private readonly DirectoryQuotaRepository.DatabaseRebuildLockHandle _inner;

            public DatabaseRebuildLockHandleAdapter(DirectoryQuotaRepository.DatabaseRebuildLockHandle inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public string? BackupPath => _inner.BackupPath;

            public void Dispose()
            {
                _inner.Dispose();
            }
        }
    }
}
