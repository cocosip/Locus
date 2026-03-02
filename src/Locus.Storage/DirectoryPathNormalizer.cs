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
    }
}
