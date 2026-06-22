using System;

namespace Locus.Core.Models
{
    /// <summary>
    /// Configuration for in-process Locus statistics aggregation.
    /// </summary>
    public class LocusStatisticsOptions
    {
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

            Output.Validate();
        }
    }
}
