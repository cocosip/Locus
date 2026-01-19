using System;
using System.Collections.Generic;
using System.IO.Abstractions;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Abstractions;
using Locus.Core.Exceptions;
using Locus.Core.Models;
using Locus.Storage.Data;
using Microsoft.Extensions.Logging;

namespace Locus.Storage
{
    /// <summary>
    /// File scheduler that manages concurrent file processing with retry logic.
    /// Ensures that different threads receive different files.
    /// </summary>
    public class FileScheduler : IFileScheduler
    {
        private readonly MetadataRepository _repository;
        private readonly IFileSystem _fileSystem;
        private readonly ILogger<FileScheduler> _logger;
        private readonly FileRetryPolicy _retryPolicy;

        /// <summary>
        /// Initializes a new instance of the <see cref="FileScheduler"/> class.
        /// </summary>
        public FileScheduler(
            MetadataRepository repository,
            IFileSystem fileSystem,
            ILogger<FileScheduler> logger,
            FileRetryPolicy? retryPolicy = null)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _fileSystem = fileSystem ?? throw new ArgumentNullException(nameof(fileSystem));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _retryPolicy = retryPolicy ?? new FileRetryPolicy();
        }

        /// <inheritdoc/>
        public async Task<FileLocation?> GetNextFileForProcessingAsync(ITenantContext tenant, CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            while (true)
            {
                var metadata = await _repository.GetNextPendingFileAsync(tenant.TenantId, ct);

                if (metadata == null)
                {
                    // No files available - return null instead of throwing exception
                    return null;
                }

                // Verify physical file exists (self-healing)
                if (!string.IsNullOrWhiteSpace(metadata.PhysicalPath) &&
                    !_fileSystem.File.Exists(metadata.PhysicalPath))
                {
                    _logger.LogWarning("Physical file missing for metadata {FileKey}, removing orphaned metadata: {PhysicalPath}",
                        metadata.FileKey, metadata.PhysicalPath);

                    await _repository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);
                    continue; // Try next file
                }

                return MapToFileLocation(metadata);
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<FileLocation>> GetNextBatchForProcessingAsync(
            ITenantContext tenant,
            int batchSize,
            CancellationToken ct)
        {
            if (tenant == null)
                throw new ArgumentNullException(nameof(tenant));

            if (batchSize <= 0)
                throw new ArgumentException("Batch size must be greater than zero", nameof(batchSize));

            var metadataList = await _repository.GetNextPendingBatchAsync(tenant.TenantId, batchSize, ct);

            var locations = new List<FileLocation>();
            foreach (var metadata in metadataList)
            {
                // Verify physical file exists (self-healing)
                if (!string.IsNullOrWhiteSpace(metadata.PhysicalPath) &&
                    !_fileSystem.File.Exists(metadata.PhysicalPath))
                {
                    _logger.LogWarning("Physical file missing for metadata {FileKey}, removing orphaned metadata: {PhysicalPath}",
                        metadata.FileKey, metadata.PhysicalPath);

                    await _repository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);
                    continue; // Skip this file
                }

                locations.Add(MapToFileLocation(metadata));
            }

            if (locations.Count == 0)
            {
                throw new NoFilesAvailableException($"No pending files available for tenant: {tenant.TenantId}");
            }

            return locations;
        }

        /// <inheritdoc/>
        public async Task MarkAsProcessingAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Need to find the metadata across all tenants to get tenantId
            var allMetadata = await _repository.GetAllAsync(ct);
            var metadata = allMetadata.FirstOrDefault(m => m.FileKey == fileKey);

            if (metadata == null)
            {
                throw new FileAlreadyProcessingException($"File not found: {fileKey}");
            }

            if (metadata.Status == FileProcessingStatus.Processing)
            {
                throw new FileAlreadyProcessingException($"File is already being processed: {fileKey}");
            }

            metadata.Status = FileProcessingStatus.Processing;
            metadata.ProcessingStartTime = DateTime.UtcNow;

            await _repository.AddOrUpdateAsync(metadata, ct);

            _logger.LogDebug("Marked file as processing: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task MarkAsCompletedAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Need to find the metadata across all tenants to get tenantId
            var allMetadata = await _repository.GetAllAsync(ct);
            var metadata = allMetadata.FirstOrDefault(m => m.FileKey == fileKey);

            if (metadata == null)
            {
                _logger.LogWarning("Attempted to mark non-existent file as completed: {FileKey}", fileKey);
                return;
            }

            // Delete physical file if it exists
            if (!string.IsNullOrWhiteSpace(metadata.PhysicalPath) && _fileSystem.File.Exists(metadata.PhysicalPath))
            {
                try
                {
                    _fileSystem.File.Delete(metadata.PhysicalPath);
                    _logger.LogDebug("Deleted physical file: {PhysicalPath}", metadata.PhysicalPath);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to delete physical file: {PhysicalPath}", metadata.PhysicalPath);
                    // Continue to remove metadata even if physical deletion fails
                }
            }

            // Remove metadata
            await _repository.RemoveAsync(metadata.TenantId, fileKey, ct);

            _logger.LogInformation("Completed and deleted file: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task MarkAsFailedAsync(string fileKey, string errorMessage, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Need to find the metadata across all tenants to get tenantId
            var allMetadata = await _repository.GetAllAsync(ct);
            var metadata = allMetadata.FirstOrDefault(m => m.FileKey == fileKey);

            if (metadata == null)
            {
                _logger.LogWarning("Attempted to mark non-existent file as failed: {FileKey}", fileKey);
                return;
            }

            metadata.RetryCount++;
            metadata.LastError = errorMessage;
            metadata.LastFailedAt = DateTime.UtcNow;
            metadata.ProcessingStartTime = null;

            if (metadata.RetryCount >= _retryPolicy.MaxRetryCount)
            {
                // Exceeded max retries, mark as permanently failed
                metadata.Status = FileProcessingStatus.PermanentlyFailed;
                metadata.AvailableForProcessingAt = null;

                _logger.LogError("File permanently failed after {RetryCount} retries: {FileKey}, Error: {Error}",
                    metadata.RetryCount, fileKey, errorMessage);
            }
            else
            {
                // Return to pending status for retry
                metadata.Status = FileProcessingStatus.Pending;

                // Calculate retry delay with exponential backoff
                var delay = CalculateRetryDelay(metadata.RetryCount);
                metadata.AvailableForProcessingAt = DateTime.UtcNow.Add(delay);

                _logger.LogWarning("File marked as failed (retry {RetryCount}/{MaxRetries}), will retry after {Delay}: {FileKey}, Error: {Error}",
                    metadata.RetryCount, _retryPolicy.MaxRetryCount, delay, fileKey, errorMessage);
            }

            await _repository.AddOrUpdateAsync(metadata, ct);
        }

        /// <inheritdoc/>
        public async Task ResetProcessingStatusAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Need to find the metadata across all tenants to get tenantId
            var allMetadata = await _repository.GetAllAsync(ct);
            var metadata = allMetadata.FirstOrDefault(m => m.FileKey == fileKey);

            if (metadata == null)
            {
                _logger.LogWarning("Attempted to reset non-existent file: {FileKey}", fileKey);
                return;
            }

            metadata.Status = FileProcessingStatus.Pending;
            metadata.ProcessingStartTime = null;
            metadata.AvailableForProcessingAt = null;
            metadata.RetryCount = 0;
            metadata.LastError = null;
            metadata.LastFailedAt = null;

            await _repository.AddOrUpdateAsync(metadata, ct);

            _logger.LogInformation("Reset processing status for file: {FileKey}", fileKey);
        }

        /// <inheritdoc/>
        public async Task<FileProcessingStatus> GetFileStatusAsync(string fileKey, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("FileKey cannot be empty", nameof(fileKey));

            // Need to find the metadata across all tenants to get tenantId
            var allMetadata = await _repository.GetAllAsync(ct);
            var metadata = allMetadata.FirstOrDefault(m => m.FileKey == fileKey);

            if (metadata == null)
            {
                throw new FileAlreadyProcessingException($"File not found: {fileKey}");
            }

            return metadata.Status;
        }

        /// <summary>
        /// Calculates the retry delay using exponential backoff.
        /// </summary>
        private TimeSpan CalculateRetryDelay(int retryCount)
        {
            if (!_retryPolicy.UseExponentialBackoff)
            {
                return _retryPolicy.InitialRetryDelay;
            }

            var delay = TimeSpan.FromMilliseconds(
                _retryPolicy.InitialRetryDelay.TotalMilliseconds * Math.Pow(2, retryCount - 1));

            // Cap at maximum delay
            if (delay > _retryPolicy.MaxRetryDelay)
            {
                delay = _retryPolicy.MaxRetryDelay;
            }

            return delay;
        }

        /// <inheritdoc/>
        public async Task<int> CleanupOrphanedMetadataAsync(CancellationToken ct)
        {
            var allMetadata = await _repository.GetAllAsync(ct);
            var removedCount = 0;

            foreach (var metadata in allMetadata)
            {
                // Check if physical file exists
                if (!string.IsNullOrWhiteSpace(metadata.PhysicalPath) &&
                    !_fileSystem.File.Exists(metadata.PhysicalPath))
                {
                    _logger.LogInformation("Removing orphaned metadata for missing file: {FileKey}, Path: {PhysicalPath}",
                        metadata.FileKey, metadata.PhysicalPath);

                    await _repository.RemoveAsync(metadata.TenantId, metadata.FileKey, ct);
                    removedCount++;
                }
            }

            if (removedCount > 0)
            {
                _logger.LogInformation("Cleaned up {Count} orphaned metadata records", removedCount);
            }

            return removedCount;
        }

        /// <summary>
        /// Maps FileMetadata to FileLocation.
        /// </summary>
        private FileLocation MapToFileLocation(FileMetadata metadata)
        {
            return new FileLocation
            {
                FileKey = metadata.FileKey,
                TenantId = metadata.TenantId,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                CreatedAt = metadata.CreatedAt,
                Status = metadata.Status,
                RetryCount = metadata.RetryCount,
                LastFailedAt = metadata.LastFailedAt,
                LastError = metadata.LastError
            };
        }
    }
}
