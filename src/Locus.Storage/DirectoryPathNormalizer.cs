using System;
using System.IO;

namespace Locus.Storage
{
    /// <summary>
    /// Normalizes directory paths used by metadata and directory-quota accounting.
    /// </summary>
    internal static class DirectoryPathNormalizer
    {
        public static string Normalize(string? directoryPath)
        {
            if (string.IsNullOrWhiteSpace(directoryPath))
                return "/";

            var normalized = (directoryPath ?? string.Empty).Trim().Replace("\\", "/");
            while (normalized.Contains("//"))
                normalized = normalized.Replace("//", "/");

            if (string.IsNullOrWhiteSpace(normalized) || normalized == ".")
                return "/";

            return normalized.StartsWith("/", StringComparison.Ordinal)
                ? normalized
                : "/" + normalized.TrimStart('/');
        }

        public static string NormalizeFromRelativePath(string? relativeDirectoryPath)
        {
            return Normalize(relativeDirectoryPath);
        }

        public static string NormalizeFromPhysicalPath(string tenantRootPath, string? physicalDirectoryPath)
        {
            if (string.IsNullOrWhiteSpace(physicalDirectoryPath))
                return "/";

            var normalizedRoot = NormalizeSeparators(tenantRootPath).TrimEnd('/');
            var normalizedDirectory = NormalizeSeparators(physicalDirectoryPath ?? string.Empty);

            if (string.IsNullOrWhiteSpace(normalizedRoot))
                return Normalize(normalizedDirectory);

            if (string.Equals(normalizedDirectory, normalizedRoot, StringComparison.OrdinalIgnoreCase))
                return "/";

            var tenantPrefix = normalizedRoot + "/";
            if (normalizedDirectory.StartsWith(tenantPrefix, StringComparison.OrdinalIgnoreCase))
                return Normalize(normalizedDirectory.Substring(tenantPrefix.Length));

            return Normalize(normalizedDirectory);
        }

        public static string NormalizeRecoveredLogicalDirectoryPath(
            string tenantRootPath,
            string? physicalPath,
            string? fileKey)
        {
            var physicalDirectoryPath = string.IsNullOrWhiteSpace(physicalPath)
                ? null
                : Path.GetDirectoryName(physicalPath);
            var recoveredPath = NormalizeFromPhysicalPath(tenantRootPath, physicalDirectoryPath);
            if (recoveredPath == "/")
                return recoveredPath;

            var trimmedPath = recoveredPath.Trim('/');
            if (trimmedPath.Length == 0)
                return "/";

            var segments = trimmedPath.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
            var shardSegmentCount = CountLeadingShardSegments(segments, fileKey);
            if (shardSegmentCount <= 0)
                return recoveredPath;

            if (shardSegmentCount >= segments.Length)
                return "/";

            var remainingSegments = new string[segments.Length - shardSegmentCount];
            Array.Copy(segments, shardSegmentCount, remainingSegments, 0, remainingSegments.Length);
            return "/" + string.Join("/", remainingSegments);
        }

        private static string NormalizeSeparators(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                return string.Empty;

            try
            {
                path = Path.GetFullPath(path);
            }
            catch
            {
                // Fall back to raw path.
            }

            return path.Trim().Replace("\\", "/");
        }

        private static int CountLeadingShardSegments(string[] segments, string? fileKey)
        {
            if (!LooksLikeGeneratedStorageKey(fileKey))
                return 0;

            var normalizedFileKey = fileKey!;
            var maxComparableSegments = Math.Min(segments.Length, normalizedFileKey.Length / 2);
            var shardSegmentCount = 0;

            for (var i = 0; i < maxComparableSegments; i++)
            {
                if (segments[i].Length != 2)
                    break;

                var expectedSegment = normalizedFileKey.Substring(i * 2, 2);
                if (!string.Equals(segments[i], expectedSegment, StringComparison.OrdinalIgnoreCase))
                    break;

                shardSegmentCount++;
            }

            return shardSegmentCount;
        }

        private static bool LooksLikeGeneratedStorageKey(string? fileKey)
        {
            if (string.IsNullOrWhiteSpace(fileKey) || fileKey!.Length != 32)
                return false;

            foreach (var ch in fileKey)
            {
                var isHex = (ch >= '0' && ch <= '9')
                    || (ch >= 'a' && ch <= 'f')
                    || (ch >= 'A' && ch <= 'F');
                if (!isHex)
                    return false;
            }

            return true;
        }
    }
}
