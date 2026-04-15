using System;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Models;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Adapts <see cref="MetadataRepository"/> to the write-side projection store contract.
    /// </summary>
    public sealed class MetadataRepositoryQueueProjectionWriteStore : IQueueProjectionWriteStore, IQueueProjectionWritePathDiagnostics
    {
        private readonly MetadataRepository _repository;

        /// <summary>
        /// Initializes a new instance of the <see cref="MetadataRepositoryQueueProjectionWriteStore"/> class.
        /// </summary>
        public MetadataRepositoryQueueProjectionWriteStore(MetadataRepository repository)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
        }

        /// <inheritdoc/>
        public Task QueueProjectedFileAsync(FileMetadata metadata, CancellationToken ct = default)
        {
            return _repository.AddOrUpdateAsync(metadata, ct);
        }

        /// <inheritdoc/>
        public QueueProjectionWritePathStatistics GetWritePathStatisticsSnapshot()
        {
            return _repository.GetWritePathStatisticsSnapshot();
        }
    }
}
