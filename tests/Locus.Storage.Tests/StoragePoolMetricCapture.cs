using System;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Locus.Storage.Tests
{
    internal sealed class StoragePoolMetricCapture : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly ConcurrentDictionary<string, int> _measurementCounts;

        public StoragePoolMetricCapture()
        {
            _measurementCounts = new ConcurrentDictionary<string, int>(StringComparer.Ordinal);
            _listener = new MeterListener();
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (string.Equals(instrument.Meter.Name, "Locus.Storage.StoragePool", StringComparison.Ordinal))
                    listener.EnableMeasurementEvents(instrument);
            };

            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                _measurementCounts.AddOrUpdate(instrument.Name, 1, (_, current) => current + 1);
            });

            _listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
            {
                _measurementCounts.AddOrUpdate(instrument.Name, 1, (_, current) => current + 1);
            });

            _listener.Start();
        }

        public int GetMeasurementCount(string instrumentName)
        {
            return _measurementCounts.TryGetValue(instrumentName, out var value) ? value : 0;
        }

        public void Dispose()
        {
            _listener.Dispose();
        }
    }
}
