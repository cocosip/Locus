using System;
using System.IO;
using System.Linq;

namespace Locus.FileSystem
{
    /// <summary>
    /// Provides path sanitization and validation to prevent directory traversal attacks.
    /// </summary>
    public static class FileSystemPathSanitizer
    {
        private static readonly char[] InvalidPathChars = Path.GetInvalidPathChars();
        private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();

        /// <summary>
        /// Validates that a path is safe and does not contain directory traversal attempts.
        /// </summary>
        /// <param name="basePath">The base directory path that should contain the file.</param>
        /// <param name="relativePath">The relative path to validate.</param>
        /// <returns>True if the path is safe, false otherwise.</returns>
        public static bool IsPathSafe(string basePath, string relativePath)
        {
            if (string.IsNullOrWhiteSpace(basePath))
                throw new ArgumentException("Base path cannot be empty", nameof(basePath));

            if (string.IsNullOrWhiteSpace(relativePath))
                throw new ArgumentException("Relative path cannot be empty", nameof(relativePath));

            try
            {
                // Get full paths
                var fullBasePath = Path.GetFullPath(basePath);
                var combinedPath = Path.Combine(basePath, relativePath);
                var fullCombinedPath = Path.GetFullPath(combinedPath);

                // Ensure the combined path starts with the base path (prevents traversal)
                return fullCombinedPath.StartsWith(fullBasePath, StringComparison.OrdinalIgnoreCase);
            }
            catch
            {
                // Any exception means the path is invalid
                return false;
            }
        }

        /// <summary>
        /// Sanitizes a file name by removing invalid characters.
        /// </summary>
        /// <param name="fileName">The file name to sanitize.</param>
        /// <returns>A sanitized file name.</returns>
        public static string SanitizeFileName(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
                throw new ArgumentException("File name cannot be empty", nameof(fileName));

            // Remove invalid characters
            var sanitized = string.Concat(fileName.Split(InvalidFileNameChars));

            // Remove any path separators
            sanitized = sanitized.Replace("/", "").Replace("\\", "");

            // Remove leading/trailing dots and spaces
            sanitized = sanitized.Trim('.', ' ');

            if (string.IsNullOrWhiteSpace(sanitized))
                throw new ArgumentException("File name contains only invalid characters", nameof(fileName));

            return sanitized;
        }

        /// <summary>
        /// Validates and sanitizes a relative path.
        /// </summary>
        /// <param name="relativePath">The relative path to sanitize.</param>
        /// <returns>A sanitized relative path.</returns>
        public static string SanitizeRelativePath(string relativePath)
        {
            if (string.IsNullOrWhiteSpace(relativePath))
                throw new ArgumentException("Relative path cannot be empty", nameof(relativePath));

            // Remove invalid path characters
            var sanitized = string.Concat(relativePath.Split(InvalidPathChars));

            // Normalize path separators to forward slashes
            sanitized = sanitized.Replace('\\', '/');

            // Remove any ".." or "." segments
            var segments = sanitized.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries)
                .Where(s => s != "." && s != "..")
                .ToArray();

            sanitized = string.Join("/", segments);

            if (string.IsNullOrWhiteSpace(sanitized))
                throw new ArgumentException("Relative path contains only invalid characters", nameof(relativePath));

            return sanitized;
        }

        /// <summary>
        /// Combines a base path with a relative path safely.
        /// Validates that the result does not escape the base path.
        /// </summary>
        /// <param name="basePath">The base directory path.</param>
        /// <param name="relativePath">The relative path to combine.</param>
        /// <returns>The combined full path.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the path would escape the base path.</exception>
        public static string CombinePathSafely(string basePath, string relativePath)
        {
            if (!IsPathSafe(basePath, relativePath))
            {
                throw new InvalidOperationException(
                    $"Path traversal attempt detected: base='{basePath}', relative='{relativePath}'");
            }

            return Path.Combine(basePath, relativePath);
        }

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
                var fullBasePath = Path.GetFullPath(basePath);
                var normalizedFullPath = Path.GetFullPath(fullPath);

                return normalizedFullPath.StartsWith(fullBasePath, StringComparison.OrdinalIgnoreCase);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Generates a safe file path for storing a file key within a base path.
        /// </summary>
        /// <param name="basePath">The base directory path.</param>
        /// <param name="fileKey">The unique file key.</param>
        /// <returns>A safe full path for the file.</returns>
        public static string GenerateSafeFilePath(string basePath, string fileKey)
        {
            if (string.IsNullOrWhiteSpace(basePath))
                throw new ArgumentException("Base path cannot be empty", nameof(basePath));

            if (string.IsNullOrWhiteSpace(fileKey))
                throw new ArgumentException("File key cannot be empty", nameof(fileKey));

            // Sanitize the file key
            var sanitizedKey = SanitizeFileName(fileKey);

            // Combine with base path
            return Path.Combine(basePath, sanitizedKey);
        }
    }
}
