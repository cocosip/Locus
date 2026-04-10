using System;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Locus.Storage.Tests
{
    internal sealed class QueueJournalMetricCapture : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly ConcurrentDictionary<string, long> _counts;

        public QueueJournalMetricCapture()
        {
            _counts = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
            _listener = new MeterListener();
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (string.Equals(instrument.Meter.Name, "Locus.Storage.QueueJournal", StringComparison.Ordinal))
                    listener.EnableMeasurementEvents(instrument);
            };

            _listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                _counts.AddOrUpdate(instrument.Name, measurement, (_, current) => current + measurement);
            });

            _listener.Start();
        }

        public long GetCount(string instrumentName)
        {
            return _counts.TryGetValue(instrumentName, out var value) ? value : 0;
        }

        public void Dispose()
        {
            _listener.Dispose();
        }
    }
}
