using System;

namespace Locus.Storage
{
    /// <summary>
    /// Validates tenant identifiers before they are used as filesystem path segments.
    /// </summary>
    internal static class TenantIdPathValidator
    {
        public static void Validate(string? tenantId, string paramName)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", paramName);

            var value = tenantId!;
            if (value.IndexOf('/') >= 0 || value.IndexOf('\\') >= 0 || value.Contains(".."))
                throw new ArgumentException($"TenantId contains invalid path characters: '{value}'", paramName);
        }
    }
}
