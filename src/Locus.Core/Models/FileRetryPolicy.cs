using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Defines the retry policy for failed file processing operations.
    /// </summary>
    public class FileRetryPolicy
    {
        /// <summary>
        /// Gets or sets the maximum number of retry attempts. Default is 3.
        /// </summary>
        public int MaxRetryCount { get; set; } = 3;

        /// <summary>
        /// Gets or sets the initial delay before the first retry. Default is 5 seconds.
        /// </summary>
        public TimeSpan InitialRetryDelay { get; set; } = TimeSpan.FromSeconds(5);

        /// <summary>
        /// Gets or sets whether to use exponential backoff for retry delays. Default is true.
        /// </summary>
        public bool UseExponentialBackoff { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum retry delay. Default is 5 minutes.
        /// </summary>
        public TimeSpan MaxRetryDelay { get; set; } = TimeSpan.FromMinutes(5);
    }
}
