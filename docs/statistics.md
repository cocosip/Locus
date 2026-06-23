# Locus Statistics

Locus exposes in-process statistics as a library feature. It collects and aggregates counters for file-processing workloads; the host application decides how to display, log, expose, or export them.
Locus can also output a periodic summary itself when `Statistics:Output` is enabled.

This keeps the component boundary small:

- Locus records low-overhead counters in memory.
- Host applications query `ILocusStatisticsReader`.
- Locus can optionally write a periodic logging summary.
- External monitoring continues to use `System.Diagnostics.Metrics` through OpenTelemetry, Prometheus, or `dotnet-counters`.
- Low-level diagnostics such as `GetWritePathStatisticsSnapshot()` remain separate from business statistics.

## When To Use It

Use Locus statistics when the host needs answers such as:

- How many files were written in the last 15 minutes?
- What was the average write throughput in MiB/s over a selected time range?
- How many files were dequeued or completed?
- How many SQLite metadata operations were persisted?
- How many files did a watcher discover, import, skip, or fail?

Do not use Locus statistics as a durable historical monitoring store. The current implementation is in-memory and windowed. If you need long-term retention, export snapshots from the host into your own logging or monitoring pipeline.

## Enable Statistics

Statistics are disabled by default. When disabled, Locus registers a no-op recorder, so hot paths only pay a single no-op interface call where instrumentation exists.

Add or update the `Locus:Statistics` section in appsettings:

```json
{
  "Locus": {
    "Statistics": {
      "Enabled": false,
      "WindowSize": "00:05:00",
      "Retention": "01:00:00",
      "MaxSeries": 16384,
      "Output": {
        "Enabled": false,
        "Sink": "Logging",
        "Interval": "00:01:00",
        "QueryWindow": "00:15:00",
        "IncludeEmptySnapshots": false
      },
      "Dimensions": {
        "TenantId": false,
        "VolumeId": true,
        "WatcherId": true,
        "Operation": true
      }
    }
  }
}
```

Keep high-cardinality dimensions off unless you need them. `TenantId` is disabled by default because large tenant counts can increase memory usage and monitoring cardinality. Locus never records file keys, file names, or physical paths as statistics dimensions.

The default windows are tuned for file-processing workloads rather than per-second tracing: 5-minute aggregation buckets, 1-hour in-memory retention, and optional self-output every 1 minute over the most recent 15 minutes. Use shorter windows only when you need near-real-time troubleshooting.

Statistics do not replace the existing `System.Diagnostics.Metrics` instruments. Keep external Metrics and Locus statistics separate because they have different consumers, dimensions, and aggregation semantics.

You can also enable statistics from code:

```csharp
services.AddLocus(options =>
{
    options.Statistics.Enabled = true;
    options.Statistics.WindowSize = TimeSpan.FromMinutes(5);
    options.Statistics.Retention = TimeSpan.FromHours(1);
    options.Statistics.Output.Enabled = false;
});
```

## Locus-Owned Output

Set `Statistics:Output:Enabled` to `true` when Locus should output its own periodic summary. The current built-in sink is `Logging`; it writes one aggregate line per interval from the configured `QueryWindow`.

This output runs in a background hosted service. It does not write from the file write path, SQLite persistence loop, or watcher import loop.

Example:

```json
{
  "Locus": {
    "Statistics": {
      "Enabled": true,
      "Output": {
        "Enabled": true,
        "Sink": "Logging",
        "Interval": "00:01:00",
        "QueryWindow": "00:15:00",
        "IncludeEmptySnapshots": false
      }
    }
  }
}
```

Typical log output contains one aggregate summary line:

```text
Locus statistics summary: Window=00:15:00, WriteFiles=1200, WriteMBps=8.532, WriteBytes=8053063680, DequeuedFiles=1180, ReadFiles=80, CompletedFiles=1150, SqliteOps=3600, WatcherImportedFiles=1190, WatcherImportedBytes=8053063680
```

Use Locus-owned output when the component should produce a lightweight periodic summary by itself. If the host already has a logging, metrics, or dashboard pipeline, prefer querying `ILocusStatisticsReader` and exporting the snapshot from the host.

## Query From The Host

Host applications can query the registered reader:

```csharp
var reader = serviceProvider.GetRequiredService<ILocusStatisticsReader>();
var snapshot = reader.GetSnapshot(new LocusStatisticsQuery
{
    From = DateTimeOffset.UtcNow.AddMinutes(-5),
    To = DateTimeOffset.UtcNow,
    VolumeId = "vol-001"
});

Console.WriteLine(snapshot.WriteMegabytesPerSecond);
Console.WriteLine(snapshot.WriteFileCount);
Console.WriteLine(snapshot.DequeuedFileCount);
```

Common query patterns:

```csharp
// Last 15 minutes across all tenants and volumes.
var recent = reader.GetSnapshot(new LocusStatisticsQuery
{
    From = DateTimeOffset.UtcNow.AddMinutes(-15),
    To = DateTimeOffset.UtcNow
});

// Last hour for one volume.
var volume = reader.GetSnapshot(new LocusStatisticsQuery
{
    From = DateTimeOffset.UtcNow.AddHours(-1),
    To = DateTimeOffset.UtcNow,
    VolumeId = "vol-001"
});

// Last 15 minutes for one watcher, if watcher_id dimension is enabled.
var watcher = reader.GetSnapshot(new LocusStatisticsQuery
{
    From = DateTimeOffset.UtcNow.AddMinutes(-15),
    To = DateTimeOffset.UtcNow,
    WatcherId = "watcher-vip-tenant-001"
});
```

`LocusStatisticsSnapshot` exposes common totals as strongly typed properties:

- `WriteFileCount`
- `WriteBytes`
- `WriteMegabytesPerSecond`
- `DequeuedFileCount`
- `ReadFileCount`
- `CompletedFileCount`
- `SqlitePersistedOperationCount`
- `WatcherImportedFileCount`
- `WatcherImportedBytes`
- `Measurements`

`Measurements` contains the lower-level measurement names and retained dimensions. Use it when you need generic rendering, grouping, or exporting.

## Window And Retention Semantics

`WindowSize` controls the aggregation bucket size. It is not the duration of a single file operation. For example, `00:05:00` means events are grouped into 5-minute buckets.

`Retention` controls how long in-memory buckets are kept. Queries can only return data that still exists in memory. The default `01:00:00` keeps a short operational window while avoiding a long-lived in-process history store.

`MaxSeries` caps retained time-bucket and dimension series. It must be between `1024` and `262144`; the default is `16384`. When the cap is reached, new series are dropped until older time buckets expire; existing series continue to aggregate. The default is sized for the built-in measurements with the default 5-minute bucket, 1-hour retention window, volume and watcher dimensions enabled, and tenant dimensions disabled.

`Output.QueryWindow` controls the time range used by Locus-owned periodic output. It can be larger than `WindowSize`; for example, the default 15-minute output window reads three 5-minute buckets.

## Dimensions

Dimensions are intentionally limited to avoid high-cardinality memory growth:

- `TenantId` controls `tenant_id`. It is disabled by default.
- `VolumeId` controls `volume_id`.
- `WatcherId` controls `watcher_id`.
- `Operation` controls `operation`.

Disable dimensions that you do not query. Do not add file keys, file names, or physical paths as dimensions.

## Current Measurements

The first statistics pass records:

- `storage.write.success.count`
- `storage.write.bytes`
- `storage.write.megabytes_per_second` (derived from `storage.write.bytes` and the query window)
- `storage.file.read.count`
- `storage.file.dequeued.count`
- `storage.file.completed.count`
- `metadata.sqlite.persisted.batch.count`
- `metadata.sqlite.persisted.operation.count`
- `watcher.scan.count`
- `watcher.files.discovered`
- `watcher.files.imported`
- `watcher.files.skipped`
- `watcher.files.failed`
- `watcher.bytes.imported`

## Metrics, Statistics, And Diagnostics

Use each layer for a different job:

- Metrics: external monitoring and alerting.
- Statistics: Locus-owned time-window business counters queried by the host.
- Diagnostics: low-level cumulative implementation counters for performance investigations.

Do not treat diagnostics snapshots as the public business statistics model. They are intentionally lower level and can include implementation details such as copy path counters and projection phase timings.
