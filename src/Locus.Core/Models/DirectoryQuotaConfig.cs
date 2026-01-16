namespace Locus.Core.Models
{
    /// <summary>
    /// Configuration for directory-level file count quotas.
    /// </summary>
    public class DirectoryQuotaConfig
    {
        /// <summary>
        /// Gets or sets the directory pattern to match (e.g., "tenant-*/files/*").
        /// </summary>
        public string DirectoryPattern { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the maximum number of files allowed per directory.
        /// </summary>
        public int MaxFilesPerDirectory { get; set; }

        /// <summary>
        /// Gets or sets whether this quota configuration is enabled.
        /// </summary>
        public bool Enabled { get; set; }
    }
}
