using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Adapts <see cref="MetadataRepository"/> to maintenance-oriented projection operations.
    /// </summary>
    public sealed class MetadataRepositoryProjectionMaintenanceStore : IMetadataProjectionMaintenanceStore
    {
        private readonly MetadataRepository _repository;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepositoryProjectionMaintenanceStore"/> class.
        /// </summary>
        public MetadataRepositoryProjectionMaintenanceStore(MetadataRepository repository)
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
        public Task<(long SizeBefore, long SizeAfter)> OptimizeDatabaseAsync(string tenantId, CancellationToken ct = default)
        {
            return _repository.OptimizeDatabaseAsync(tenantId, ct);
        }

        private sealed class DatabaseRebuildLockHandleAdapter : IDatabaseRebuildLockHandle
        {
            private readonly MetadataRepository.DatabaseRebuildLockHandle _inner;

            public DatabaseRebuildLockHandleAdapter(MetadataRepository.DatabaseRebuildLockHandle inner)
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
