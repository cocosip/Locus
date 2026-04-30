using System;
using Locus.Core.Models;
using Locus.Storage.Data;

namespace Locus.Storage
{
    internal static class QueueEventRecordFactory
    {
        public static QueueEventRecord CreateProcessingStarted(FileLocation location)
        {
            return new QueueEventRecord
            {
                TenantId = location.TenantId,
                FileKey = location.FileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = location.ProcessingStartTime ?? DateTime.UtcNow,
                VolumeId = location.VolumeId,
                PhysicalPath = location.PhysicalPath,
                DirectoryPath = location.DirectoryPath,
                FileSize = location.FileSize,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = location.ProcessingStartTime,
                RetryCount = location.RetryCount,
                AvailableForProcessingAtUtc = null,
                ErrorMessage = location.LastError
            };
        }

        public static QueueEventRecord CreateProcessingStarted(FileMetadata metadata)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingStarted,
                OccurredAtUtc = metadata.ProcessingStartTime ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.Processing,
                ProcessingStartTimeUtc = metadata.ProcessingStartTime,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = null,
                ErrorMessage = metadata.LastError,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension,
            };
        }

        public static QueueEventRecord CreateProcessingCompleted(
            FileMetadata metadata,
            DateTime processingStartTimeUtc,
            DateTime? completedAtUtc = null)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingCompleted,
                OccurredAtUtc = completedAtUtc ?? metadata.CompletedAt ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.Completed,
                ProcessingStartTimeUtc = processingStartTimeUtc,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = metadata.AvailableForProcessingAt,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
            };
        }

        public static QueueEventRecord CreateDeleteRequested(
            FileMetadata metadata,
            DateTime? occurredAtUtc = null)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.DeleteRequested,
                OccurredAtUtc = occurredAtUtc ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = FileProcessingStatus.DeleteRequested,
                RetryCount = metadata.RetryCount,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
            };
        }

        public static QueueEventRecord CreateProcessingFailed(
            FileMetadata metadata,
            string errorMessage,
            DateTime expectedProcessingStartTimeUtc)
        {
            return new QueueEventRecord
            {
                TenantId = metadata.TenantId,
                FileKey = metadata.FileKey,
                EventType = QueueEventType.ProcessingFailed,
                OccurredAtUtc = metadata.LastFailedAt ?? DateTime.UtcNow,
                VolumeId = metadata.VolumeId,
                PhysicalPath = metadata.PhysicalPath,
                DirectoryPath = metadata.DirectoryPath,
                FileSize = metadata.FileSize,
                Status = metadata.Status,
                ProcessingStartTimeUtc = expectedProcessingStartTimeUtc,
                RetryCount = metadata.RetryCount,
                AvailableForProcessingAtUtc = metadata.AvailableForProcessingAt,
                ErrorMessage = errorMessage,
                OriginalFileName = metadata.OriginalFileName,
                FileExtension = metadata.FileExtension
            };
        }

        public static bool IsCompletionCommittedStatus(FileProcessingStatus status)
        {
            return status == FileProcessingStatus.Completed
                || status == FileProcessingStatus.DeleteRequested
                || status == FileProcessingStatus.DeleteSucceeded;
        }
    }
}
