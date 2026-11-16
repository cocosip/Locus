using System;
using System.IO;

namespace Locus.FileSystem
{
    /// <summary>
    /// Validates paths to prevent directory traversal.
    /// </summary>
    public static class FileSystemPathSanitizer
    {
        /// <summary>
        /// Validates that a full path is within the allowed base path.
        /// </summary>
        /// <param name="basePath">The base directory path.</param>
        /// <param name="fullPath">The full path to validate.</param>
        /// <returns>True if the path is within the base path, false otherwise.</returns>
        public static bool IsPathWithinBase(string basePath, string fullPath)
        {
            if (string.IsNullOrWhiteSpace(basePath))
                throw new ArgumentException("Base path cannot be empty", nameof(basePath));

            if (string.IsNullOrWhiteSpace(fullPath))
                throw new ArgumentException("Full path cannot be empty", nameof(fullPath));

            try
            {
                var fullBasePath = Path.GetFullPath(basePath).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
                var normalizedFullPath = Path.GetFullPath(fullPath);

                // Use case-sensitive comparison on Unix, case-insensitive on Windows
                var comparison = Path.DirectorySeparatorChar == '/'
                    ? StringComparison.Ordinal
                    : StringComparison.OrdinalIgnoreCase;

                return normalizedFullPath.StartsWith(fullBasePath + Path.DirectorySeparatorChar, comparison) ||
                       normalizedFullPath.Equals(fullBasePath, comparison);
            }
            catch
            {
                return false;
            }
        }
    }
}
