using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Configures optional Locus-owned statistics output.
    /// </summary>
    public class LocusStatisticsOutputOptions
    {
        /// <summary>
        /// Gets or sets whether Locus should periodically output statistics itself.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Gets or sets the output kind. Currently only "Logging" is supported.
        /// </summary>
        public string Sink { get; set; } = "Logging";

        /// <summary>
        /// Gets or sets the output interval.
        /// </summary>
        public TimeSpan Interval { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets the time range included in each output.
        /// </summary>
        public TimeSpan QueryWindow { get; set; } = TimeSpan.FromMinutes(15);

        /// <summary>
        /// Gets or sets whether zero-valued summaries should be logged.
        /// </summary>
        public bool IncludeEmptySnapshots { get; set; }

        /// <summary>
        /// Validates output options.
        /// </summary>
        public void Validate()
        {
            if (Interval <= TimeSpan.Zero)
                throw new InvalidOperationException("Statistics Output Interval must be greater than zero.");

            if (QueryWindow <= TimeSpan.Zero)
                throw new InvalidOperationException("Statistics Output QueryWindow must be greater than zero.");

            if (!string.Equals(Sink, "Logging", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("Statistics Output Sink must be 'Logging'.");
        }
    }
}
