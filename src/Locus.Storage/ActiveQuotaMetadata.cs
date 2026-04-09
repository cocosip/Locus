using Locus.Core.Models;
using Locus.Storage.Data;

namespace Locus.Storage
{
    internal static class ActiveQuotaMetadata
    {
        public static bool CountsTowardActiveQuota(FileMetadata metadata)
        {
            return metadata.Status != FileProcessingStatus.DeadLettered;
        }
    }
}
