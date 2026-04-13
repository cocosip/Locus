using Locus.Core.Models;
using Locus.Storage.Data;

namespace Locus.Storage
{
    internal static class ActiveQuotaMetadata
    {
        public static bool CountsTowardActiveQuota(FileMetadata metadata)
        {
            switch (metadata.Status)
            {
                case FileProcessingStatus.Pending:
                case FileProcessingStatus.Processing:
                case FileProcessingStatus.Completed:
                case FileProcessingStatus.Failed:
                case FileProcessingStatus.PermanentlyFailed:
                case FileProcessingStatus.DeleteRequested:
                    return true;
                case FileProcessingStatus.DeleteSucceeded:
                case FileProcessingStatus.DeadLettered:
                default:
                    return false;
            }
        }
    }
}
