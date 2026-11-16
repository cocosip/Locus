using System;
using System.IO;

namespace Locus.Core.Validation
{
    /// <summary>
    /// Validates tenant identifiers before they are used as filesystem path segments.
    /// A tenant id becomes part of metadata, quota and journal paths, so it must be a single,
    /// portable path segment: no separators, no relative segments, no drive/root prefix and no
    /// characters the platform rejects in file names.
    /// </summary>
    public static class TenantIdValidator
    {
        private static readonly char[] InvalidFileNameChars = Path.GetInvalidFileNameChars();

        /// <summary>
        /// Throws <see cref="ArgumentException"/> when the tenant id cannot be used as a path segment.
        /// </summary>
        /// <param name="tenantId">The tenant identifier to validate.</param>
        /// <param name="paramName">The parameter name reported by the exception.</param>
        /// <exception cref="ArgumentException">Thrown when the tenant id is empty or contains path characters.</exception>
        public static void Validate(string? tenantId, string paramName)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", paramName);

            var value = tenantId!;
            if (value.IndexOf('/') >= 0
                || value.IndexOf('\\') >= 0
                || value.Contains("..")
                || value.IndexOfAny(InvalidFileNameChars) >= 0
                || Path.IsPathRooted(value))
            {
                throw new ArgumentException($"TenantId contains invalid path characters: '{value}'", paramName);
            }
        }
    }
}
