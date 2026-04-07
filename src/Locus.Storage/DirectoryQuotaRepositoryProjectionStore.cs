using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Adapts <see cref="DirectoryQuotaRepository"/> to the quota projection store contract.
    /// </summary>
    public sealed class DirectoryQuotaRepositoryProjectionStore : IQuotaProjectionStore
    {
        private readonly DirectoryQuotaRepository _repository;

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaRepositoryProjectionStore"/> class.
        /// </summary>
        public DirectoryQuotaRepositoryProjectionStore(DirectoryQuotaRepository repository)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        }

        /// <inheritdoc/>
        public Task<DirectoryQuota> GetOrCreateQuotaAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            return _repository.GetOrCreateAsync(tenantId, directoryPath, ct);
        }

        /// <inheritdoc/>
        public Task<DirectoryQuota?> GetQuotaAsync(string tenantId, string directoryPath, CancellationToken ct = default)
        {
            return _repository.GetAsync(tenantId, directoryPath, ct);
        }

        /// <inheritdoc/>
        public Task UpdateQuotaAsync(string tenantId, DirectoryQuota quota, CancellationToken ct = default)
        {
            return _repository.UpdateAsync(tenantId, quota, ct);
        }

        /// <inheritdoc/>
        public Task SetProjectedCountAsync(string tenantId, string directoryPath, int count, CancellationToken ct = default)
        {
            return _repository.SetCurrentCountAsync(tenantId, directoryPath, count, ct);
        }
    }
}
