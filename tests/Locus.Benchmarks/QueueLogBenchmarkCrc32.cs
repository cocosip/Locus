#nullable enable
using System;

namespace Locus.Benchmarks
{
    internal static class QueueLogBenchmarkCrc32
    {
        private static readonly uint[] Table = BuildTable();

        public static uint Compute(byte[] buffer)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            return Compute(buffer, 0, buffer.Length);
        }

        public static uint Compute(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0 || offset > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(offset));

            if (count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException(nameof(count));

            var crc = 0xFFFFFFFFu;
            for (var i = 0; i < count; i++)
                crc = (crc >> 8) ^ Table[(crc ^ buffer[offset + i]) & 0xFF];

            return ~crc;
        }

        private static uint[] BuildTable()
        {
            var table = new uint[256];
            for (uint i = 0; i < table.Length; i++)
            {
                var value = i;
                for (var bit = 0; bit < 8; bit++)
                {
                    value = (value & 1) == 1
                        ? (value >> 1) ^ 0xEDB88320u
                        : value >> 1;
                }

                table[i] = value;
            }

            return table;
        }
    }
}
