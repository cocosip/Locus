using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using Locus.Core.Models;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Dapper read DTO that maps SQLite snake_case column names to CLR properties.
    /// Convert to the canonical <see cref="FileMetadata"/> via <see cref="ToFileMetadata"/>.
    /// </summary>
    internal sealed class FileMetadataRow
    {
        // Column names must match the DDL exactly (snake_case).
        public string file_key { get; set; } = string.Empty;
        public string tenant_id { get; set; } = string.Empty;
        public string volume_id { get; set; } = string.Empty;
        public string physical_path { get; set; } = string.Empty;
        public string directory_path { get; set; } = string.Empty;
        public long   file_size { get; set; }
        public string created_at { get; set; } = string.Empty;
        public int    status { get; set; }
        public int    retry_count { get; set; }
        public string? last_failed_at { get; set; }
        public string? last_error { get; set; }
        public string? processing_start_time { get; set; }
        public string? available_for_processing_at { get; set; }
        public string? original_file_name { get; set; }
        public string? file_extension { get; set; }
        public string? metadata_json { get; set; }

        /// <summary>
        /// Converts this read DTO to the canonical domain model.
        /// </summary>
        public FileMetadata ToFileMetadata()
        {
            return new FileMetadata
            {
                FileKey                  = file_key,
                TenantId                 = tenant_id,
                VolumeId                 = volume_id,
                PhysicalPath             = physical_path,
                DirectoryPath            = directory_path,
                FileSize                 = file_size,
                CreatedAt                = ParseDateTime(created_at),
                Status                   = (FileProcessingStatus)status,
                RetryCount               = retry_count,
                LastFailedAt             = ParseNullableDateTime(last_failed_at),
                LastError                = last_error,
                ProcessingStartTime      = ParseNullableDateTime(processing_start_time),
                AvailableForProcessingAt = ParseNullableDateTime(available_for_processing_at),
                OriginalFileName         = original_file_name,
                FileExtension            = file_extension,
                Metadata                 = DeserializeMetadata(metadata_json)
            };
        }

        private static DateTime ParseDateTime(string s)
        {
            if (DateTime.TryParse(s, null, DateTimeStyles.RoundtripKind, out var result))
                return result;

            // Fallback: try general parsing to handle non-ISO 8601 values written by older
            // tooling or migration scripts.  If that also fails, return UTC epoch so the
            // record stays loadable and does not trigger an unnecessary DB rebuild.
            if (DateTime.TryParse(s, out result))
                return DateTime.SpecifyKind(result, DateTimeKind.Utc);

            return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }

        private static DateTime? ParseNullableDateTime(string? s)
        {
            if (string.IsNullOrEmpty(s))
                return null;

            if (DateTime.TryParse(s, null, DateTimeStyles.RoundtripKind, out var result))
                return result;

            if (DateTime.TryParse(s, out result))
                return DateTime.SpecifyKind(result, DateTimeKind.Utc);

            return null;
        }

        private static Dictionary<string, string>? DeserializeMetadata(string? json)
            => string.IsNullOrEmpty(json)
                ? null
                : JsonSerializer.Deserialize<Dictionary<string, string>>(json!);
    }
}
