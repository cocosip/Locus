using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Configuration for in-process Locus statistics aggregation.
    /// </summary>
    public class LocusStatisticsOptions
    {
        /// <summary>
        /// Default maximum number of retained time bucket and dimension series.
        /// </summary>
        public const int DefaultMaxSeries = 16_384;

        /// <summary>
        /// Minimum allowed maximum number of retained time bucket and dimension series.
        /// </summary>
        public const int MinMaxSeries = 1_024;

        /// <summary>
        /// Maximum allowed maximum number of retained time bucket and dimension series.
        /// </summary>
        public const int MaxMaxSeries = 262_144;

        /// <summary>
        /// Gets or sets whether in-process statistics are enabled.
        /// </summary>
        public bool Enabled { get; set; }

        /// <summary>
        /// Gets or sets the aggregation bucket size.
        /// </summary>
        public TimeSpan WindowSize { get; set; } = TimeSpan.FromMinutes(5);

        /// <summary>
        /// Gets or sets how long in-memory buckets are retained.
        /// </summary>
        public TimeSpan Retention { get; set; } = TimeSpan.FromHours(1);

        /// <summary>
        /// Gets or sets the maximum number of retained time bucket and dimension series.
        /// </summary>
        public int MaxSeries { get; set; } = DefaultMaxSeries;

        /// <summary>
        /// Gets or sets retained dimensions.
        /// </summary>
        public LocusStatisticsDimensionOptions Dimensions { get; set; } = new LocusStatisticsDimensionOptions();

        /// <summary>
        /// Gets or sets optional Locus-owned statistics output options.
        /// </summary>
        public LocusStatisticsOutputOptions Output { get; set; } = new LocusStatisticsOutputOptions();

        /// <summary>
        /// Validates statistics options.
        /// </summary>
        public void Validate()
        {
            if (WindowSize <= TimeSpan.Zero)
                throw new InvalidOperationException("Statistics WindowSize must be greater than zero.");

            if (Retention <= TimeSpan.Zero)
                throw new InvalidOperationException("Statistics Retention must be greater than zero.");

            if (Retention < WindowSize)
                throw new InvalidOperationException("Statistics Retention must be greater than or equal to WindowSize.");

            if (MaxSeries < MinMaxSeries || MaxSeries > MaxMaxSeries)
                throw new InvalidOperationException("Statistics MaxSeries must be between 1024 and 262144.");

            Output.Validate();
        }
    }
}
