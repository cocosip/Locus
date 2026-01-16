using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Global configuration options for the file watcher service.
    /// </summary>
    public class FileWatcherOptions
    {
        /// <summary>
        /// Gets or sets whether the file watcher service is globally enabled.
        /// When set to false, all file monitoring will be stopped regardless of individual watcher settings.
        /// Default is true.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Gets or sets the default polling interval for file watchers.
        /// Individual watchers can override this value.
        /// Default is 30 seconds.
        /// </summary>
        public TimeSpan DefaultPollingInterval { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Gets or sets the minimum polling interval allowed.
        /// Prevents setting intervals too short which could cause performance issues.
        /// Default is 5 seconds.
        /// </summary>
        public TimeSpan MinimumPollingInterval { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Gets or sets the maximum polling interval allowed.
        /// Default is 1 hour.
        /// </summary>
        public TimeSpan MaximumPollingInterval { get; set; } = TimeSpan.FromHours(1);

        /// <summary>
        /// Gets or sets the check interval when the service is globally disabled.
        /// The service will check this often whether it should resume operation.
        /// Default is 1 minute.
        /// </summary>
        public TimeSpan DisabledCheckInterval { get; set; } = TimeSpan.FromMinutes(1);
    }
}
