using System.Collections.Generic;

namespace Locus.Core.Models
{
    /// <summary>
    /// A single aggregated statistics measurement.
    /// </summary>
    public sealed class LocusStatisticsMeasurement
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="LocusStatisticsMeasurement"/> class.
        /// </summary>
        public LocusStatisticsMeasurement(
            string name,
            double value,
            IReadOnlyDictionary<string, string> dimensions)
        {
            Name = name;
            Value = value;
            Dimensions = dimensions;
        }

        /// <summary>
        /// Gets the measurement name.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the aggregated value.
        /// </summary>
        public double Value { get; }

        /// <summary>
        /// Gets retained dimensions.
        /// </summary>
        public IReadOnlyDictionary<string, string> Dimensions { get; }
    }
}
