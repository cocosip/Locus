using System.Collections.Generic;
using System.Diagnostics.Metrics;

namespace Locus.FileSystem
{
    internal static class LocalFileSystemVolumeMetrics
    {
        private static readonly Meter Meter = new Meter("Locus.FileSystem.StorageVolume", "1.0.0");
        private static readonly Counter<long> WriteCounter = Meter.CreateCounter<long>(
            "locus.storage_volume.write.count",
            unit: "{write}",
            description: "Number of writes observed by LocalFileSystemVolume.");
        private static readonly Counter<long> DirectFastPathWriteCounter = Meter.CreateCounter<long>(
            "locus.storage_volume.write.direct_fast_path.count",
            unit: "{write}",
            description: "Number of writes that used the direct MemoryStream fast path.");
        private static readonly Counter<long> SynchronousSeekableFileWriteCounter = Meter.CreateCounter<long>(
            "locus.storage_volume.write.sync_seekable_file.count",
            unit: "{write}",
            description: "Number of writes that used the synchronous seekable FileStream copy path.");
        private static readonly Histogram<long> WriteBytesHistogram = Meter.CreateHistogram<long>(
            "locus.storage_volume.write.bytes",
            unit: "By",
            description: "Bytes requested per LocalFileSystemVolume write.");
        private static readonly Histogram<long> DirectFastPathBytesHistogram = Meter.CreateHistogram<long>(
            "locus.storage_volume.write.direct_fast_path.bytes",
            unit: "By",
            description: "Bytes written through the direct MemoryStream fast path.");

        public static void RecordWrite(
            string volumeId,
            string sourceKind,
            bool hasVisibleBuffer,
            bool usesDirectFastPath,
            bool usesSynchronousSmallWritePath,
            bool usesSynchronousSeekableFileWritePath,
            long? bytes)
        {
            var tags = CreateTags(
                volumeId,
                sourceKind,
                hasVisibleBuffer,
                usesDirectFastPath,
                usesSynchronousSmallWritePath,
                usesSynchronousSeekableFileWritePath);

            WriteCounter.Add(1, tags);
            if (bytes.HasValue && bytes.Value >= 0)
                WriteBytesHistogram.Record(bytes.Value, tags);

            if (usesDirectFastPath)
            {
                DirectFastPathWriteCounter.Add(1, tags);
                if (bytes.HasValue && bytes.Value >= 0)
                    DirectFastPathBytesHistogram.Record(bytes.Value, tags);
            }

            if (usesSynchronousSeekableFileWritePath)
                SynchronousSeekableFileWriteCounter.Add(1, tags);
        }

        private static KeyValuePair<string, object?>[] CreateTags(
            string volumeId,
            string sourceKind,
            bool hasVisibleBuffer,
            bool usesDirectFastPath,
            bool usesSynchronousSmallWritePath,
            bool usesSynchronousSeekableFileWritePath)
        {
            return new[]
            {
                new KeyValuePair<string, object?>("volume_id", volumeId),
                new KeyValuePair<string, object?>("source_kind", sourceKind),
                new KeyValuePair<string, object?>("visible_buffer", hasVisibleBuffer),
                new KeyValuePair<string, object?>("direct_fast_path", usesDirectFastPath),
                new KeyValuePair<string, object?>("sync_small_write_path", usesSynchronousSmallWritePath),
                new KeyValuePair<string, object?>("sync_seekable_file_path", usesSynchronousSeekableFileWritePath),
            };
        }
    }
}
