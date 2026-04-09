using System;
using System.Collections.Generic;
using Locus.Storage.Data;

namespace Locus.Storage
{
    /// <summary>
    /// Stores durable projection flags inside FileMetadata.Metadata.
    /// </summary>
    internal static class QueueProjectionMetadataState
    {
        private const string AcceptedProjectionAppliedKey = "queue.accepted_projection_applied";
        private const string DeadLetterProjectionAppliedKey = "queue.dead_letter_projection_applied";

        public static bool IsAcceptedProjectionApplied(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (metadata.Metadata == null)
                return false;

            return metadata.Metadata.TryGetValue(AcceptedProjectionAppliedKey, out var value)
                && string.Equals(value, bool.TrueString, StringComparison.OrdinalIgnoreCase);
        }

        public static void MarkAcceptedProjectionPending(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            GetWritableMetadata(metadata)[AcceptedProjectionAppliedKey] = bool.FalseString;
        }

        public static void MarkAcceptedProjectionApplied(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            GetWritableMetadata(metadata)[AcceptedProjectionAppliedKey] = bool.TrueString;
        }

        public static bool IsDeadLetterProjectionApplied(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (metadata.Metadata == null)
                return false;

            return metadata.Metadata.TryGetValue(DeadLetterProjectionAppliedKey, out var value)
                && string.Equals(value, bool.TrueString, StringComparison.OrdinalIgnoreCase);
        }

        public static void MarkDeadLetterProjectionPending(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            GetWritableMetadata(metadata)[DeadLetterProjectionAppliedKey] = bool.FalseString;
        }

        public static void MarkDeadLetterProjectionApplied(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            GetWritableMetadata(metadata)[DeadLetterProjectionAppliedKey] = bool.TrueString;
        }

        public static void ClearDeadLetterProjection(FileMetadata metadata)
        {
            if (metadata == null)
                throw new ArgumentNullException(nameof(metadata));

            if (metadata.Metadata == null)
                return;

            metadata.Metadata = new Dictionary<string, string>(metadata.Metadata, StringComparer.Ordinal);
            metadata.Metadata.Remove(DeadLetterProjectionAppliedKey);
        }

        private static IDictionary<string, string> GetWritableMetadata(FileMetadata metadata)
        {
            if (metadata.Metadata == null)
            {
                metadata.Metadata = new Dictionary<string, string>(StringComparer.Ordinal);
                return metadata.Metadata;
            }

            metadata.Metadata = new Dictionary<string, string>(metadata.Metadata, StringComparer.Ordinal);
            return metadata.Metadata;
        }
    }
}
