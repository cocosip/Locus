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
        private static readonly Histogram<long> WriteBytesHistogram = Meter.CreateHistogram<long>(
            "locus.storage_volume.write.bytes",
            unit: "By",
            description: "Bytes requested per LocalFileSystemVolume write.");

        public static void RecordWrite(
            string volumeId,
            long? bytes)
        {
            var tags = new[]
            {
                new KeyValuePair<string, object?>("volume_id", volumeId),
            };

            WriteCounter.Add(1, tags);
            if (bytes.HasValue && bytes.Value >= 0)
                WriteBytesHistogram.Record(bytes.Value, tags);
        }
    }
}
