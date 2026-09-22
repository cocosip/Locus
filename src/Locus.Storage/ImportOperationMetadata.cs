using System;
using System.Collections.Generic;
using Locus.Storage.Data;

namespace Locus.Storage
{
    internal static class ImportOperationMetadata
    {
        public const string Key = "locus.import_operation_id";

        public static string? GetOperationId(FileMetadata metadata)
        {
            if (metadata.Metadata == null
                || !metadata.Metadata.TryGetValue(Key, out var operationId)
                || string.IsNullOrWhiteSpace(operationId))
            {
                return null;
            }

            return operationId;
        }

        public static void SetOperationId(FileMetadata metadata, string? operationId)
        {
            if (string.IsNullOrWhiteSpace(operationId))
                return;

            metadata.Metadata = metadata.Metadata == null
                ? new Dictionary<string, string>(StringComparer.Ordinal)
                : new Dictionary<string, string>(metadata.Metadata, StringComparer.Ordinal);
            metadata.Metadata[Key] = operationId!;
        }
    }
}
