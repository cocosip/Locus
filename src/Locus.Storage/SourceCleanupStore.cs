using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Locus.Core.Models;
using Microsoft.Data.Sqlite;

namespace Locus.Storage
{
    /// <summary>
    /// SQLite-backed store containing only active source cleanup work.
    /// Successful cleanup removes its row, so the store does not grow with import volume.
    /// </summary>
    public sealed class SourceCleanupStore : ISourceCleanupStore, IDisposable
    {
        private readonly string _databasePath;
        private readonly object _gate = new object();
        private readonly SqliteConnection _connection;

        /// <summary>
        /// Opens or creates the source cleanup SQLite database.
        /// </summary>
        /// <param name="databasePath">The database file path.</param>
        public SourceCleanupStore(string databasePath)
        {
            if (string.IsNullOrWhiteSpace(databasePath))
                throw new ArgumentException("Source cleanup database path cannot be empty.", nameof(databasePath));

            _databasePath = Path.GetFullPath(databasePath);
            var directory = Path.GetDirectoryName(_databasePath);
            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            _connection = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = _databasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Shared,
                Pooling = false
            }.ToString());
            _connection.Open();
            InitializeSchema();
        }

        /// <inheritdoc />
        public Task<SourceCleanupJob?> GetActiveAsync(string sourcePath, string fingerprint, CancellationToken ct = default)
        {
            return Task.FromResult(GetActive(sourcePath, fingerprint, ct));
        }

        /// <inheritdoc />
        public Task<long> UpsertAsync(SourceCleanupJob job, CancellationToken ct = default)
        {
            return Task.FromResult(Upsert(job, ct));
        }

        /// <summary>Reserves a bounded source cleanup slot before import.</summary>
        public Task<bool> TryReserveAsync(
            SourceCleanupJob job,
            int maxActiveJobs,
            DateTime reservationUntilUtc,
            CancellationToken ct = default)
        {
            return Task.FromResult(TryReserve(job, maxActiveJobs, reservationUntilUtc, ct));
        }

        /// <summary>Gets the current number of active source cleanup rows.</summary>
        public Task<int> GetActiveCountAsync(CancellationToken ct = default)
        {
            return Task.FromResult(GetActiveCount(ct));
        }

        /// <inheritdoc />
        public Task<IReadOnlyList<SourceCleanupJob>> GetDueAsync(DateTime nowUtc, int limit, CancellationToken ct = default)
        {
            return Task.FromResult<IReadOnlyList<SourceCleanupJob>>(GetDue(nowUtc, limit, null, ct));
        }

        /// <summary>Gets due jobs together with stale import reservations.</summary>
        public Task<IReadOnlyList<SourceCleanupJob>> GetDueAsync(
            DateTime nowUtc,
            int limit,
            DateTime staleImportCutoffUtc,
            CancellationToken ct = default)
        {
            return Task.FromResult<IReadOnlyList<SourceCleanupJob>>(
                GetDue(nowUtc, limit, staleImportCutoffUtc, ct));
        }

        /// <inheritdoc />
        public Task<bool> TryClaimAsync(long id, DateTime nowUtc, DateTime leaseUntilUtc, CancellationToken ct = default)
        {
            return Task.FromResult(TryClaim(id, nowUtc, leaseUntilUtc, ct));
        }

        /// <inheritdoc />
        public Task UpdateAsync(SourceCleanupJob job, CancellationToken ct = default)
        {
            Update(job, ct);
            return Task.CompletedTask;
        }

        /// <inheritdoc />
        public Task RemoveAsync(long id, CancellationToken ct = default)
        {
            Remove(id, ct);
            return Task.CompletedTask;
        }

        /// <summary>Removes terminal suppression rows older than the supplied cutoff.</summary>
        public Task<int> PruneTerminalAsync(DateTime cutoffUtc, int limit, CancellationToken ct = default)
        {
            return Task.FromResult(PruneTerminal(cutoffUtc, limit, ct));
        }

        /// <summary>Runs SQLite VACUUM and returns the database size before and after.</summary>
        public Task<(long SizeBefore, long SizeAfter)> OptimizeAsync(CancellationToken ct = default)
        {
            return Task.FromResult(Optimize(ct));
        }

        private void InitializeSchema()
        {
            lock (_gate)
            {
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"
CREATE TABLE IF NOT EXISTS source_cleanup_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watcher_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    file_key TEXT NOT NULL,
    import_operation_id TEXT,
    action INTEGER NOT NULL,
    move_target_path TEXT,
    failure_directory TEXT,
    max_attempts INTEGER NOT NULL,
    retry_initial_delay_ticks INTEGER NOT NULL,
    retry_max_delay_ticks INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL,
    state INTEGER NOT NULL,
    next_attempt_utc TEXT,
    last_error TEXT,
    created_at_utc TEXT NOT NULL,
    updated_at_utc TEXT NOT NULL,
    lease_until_utc TEXT,
    UNIQUE(watcher_id, source_path)
);
CREATE INDEX IF NOT EXISTS idx_source_cleanup_due
    ON source_cleanup_jobs(state, next_attempt_utc, lease_until_utc);";
                    command.ExecuteNonQuery();
                }

                EnsureColumnExists("import_operation_id", "TEXT");
            }
        }

        private void EnsureColumnExists(string columnName, string columnDefinition)
        {
            if (ColumnExists(columnName))
                return;

            try
            {
                using (var alter = _connection.CreateCommand())
                {
                    alter.CommandText = $"ALTER TABLE source_cleanup_jobs ADD COLUMN {columnName} {columnDefinition};";
                    alter.ExecuteNonQuery();
                }
            }
            catch (SqliteException) when (ColumnExists(columnName))
            {
                // Another store instance completed the same idempotent schema upgrade.
            }
        }

        private bool ColumnExists(string columnName)
        {
            using (var query = _connection.CreateCommand())
            {
                query.CommandText = "PRAGMA table_info(source_cleanup_jobs);";
                using (var reader = query.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        if (string.Equals(reader.GetString(1), columnName, StringComparison.OrdinalIgnoreCase))
                            return true;
                    }
                }
            }

            return false;
        }

        private SourceCleanupJob? GetActive(string sourcePath, string fingerprint, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(sourcePath) || string.IsNullOrWhiteSpace(fingerprint))
                return null;

            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"SELECT * FROM source_cleanup_jobs
WHERE source_path = $source_path LIMIT 1";
                    command.Parameters.AddWithValue("$source_path", sourcePath);
                    using (var reader = command.ExecuteReader())
                        return reader.Read() ? ReadJob(reader) : null;
                }
            }
        }

        private long Upsert(SourceCleanupJob job, CancellationToken ct)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (string.IsNullOrWhiteSpace(job.SourcePath) || string.IsNullOrWhiteSpace(job.Fingerprint))
                throw new ArgumentException("Cleanup job source path and fingerprint are required.", nameof(job));

            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                var now = job.UpdatedAtUtc == default(DateTime) ? DateTime.UtcNow : job.UpdatedAtUtc;
                job.UpdatedAtUtc = now;
                if (job.CreatedAtUtc == default(DateTime))
                    job.CreatedAtUtc = now;

                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"
INSERT INTO source_cleanup_jobs
 (watcher_id, tenant_id, source_path, fingerprint, file_key, import_operation_id, action, move_target_path,
 failure_directory, max_attempts, retry_initial_delay_ticks, retry_max_delay_ticks,
 attempt_count, state, next_attempt_utc, last_error, created_at_utc, updated_at_utc, lease_until_utc)
 VALUES ($watcher_id, $tenant_id, $source_path, $fingerprint, $file_key, $import_operation_id, $action, $move_target_path,
 $failure_directory, $max_attempts, $retry_initial_delay_ticks, $retry_max_delay_ticks,
 $attempt_count, $state, $next_attempt_utc, $last_error, $created_at_utc, $updated_at_utc, $lease_until_utc)
ON CONFLICT(watcher_id, source_path) DO UPDATE SET
 tenant_id = excluded.tenant_id,
 fingerprint = excluded.fingerprint,
 file_key = excluded.file_key,
 import_operation_id = excluded.import_operation_id,
 action = excluded.action,
 move_target_path = excluded.move_target_path,
 failure_directory = excluded.failure_directory,
 max_attempts = excluded.max_attempts,
 retry_initial_delay_ticks = excluded.retry_initial_delay_ticks,
 retry_max_delay_ticks = excluded.retry_max_delay_ticks,
 attempt_count = excluded.attempt_count,
 state = excluded.state,
 next_attempt_utc = excluded.next_attempt_utc,
 last_error = excluded.last_error,
 updated_at_utc = excluded.updated_at_utc,
 lease_until_utc = excluded.lease_until_utc;
SELECT id FROM source_cleanup_jobs WHERE watcher_id = $watcher_id AND source_path = $source_path;";
                    Bind(command, job);
                    return Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture);
                }
            }
        }

        private bool TryReserve(
            SourceCleanupJob job,
            int maxActiveJobs,
            DateTime reservationUntilUtc,
            CancellationToken ct)
        {
            if (job == null)
                throw new ArgumentNullException(nameof(job));
            if (string.IsNullOrWhiteSpace(job.WatcherId)
                || string.IsNullOrWhiteSpace(job.SourcePath)
                || string.IsNullOrWhiteSpace(job.Fingerprint))
            {
                throw new ArgumentException("Reservation watcher, source path and fingerprint are required.", nameof(job));
            }

            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var transaction = _connection.BeginTransaction())
                using (var countCommand = _connection.CreateCommand())
                {
                    countCommand.Transaction = transaction;
                    countCommand.CommandText = "SELECT COUNT(*) FROM source_cleanup_jobs;";
                    var activeCount = Convert.ToInt32(countCommand.ExecuteScalar(), CultureInfo.InvariantCulture);
                    if (maxActiveJobs > 0 && activeCount >= maxActiveJobs)
                    {
                        transaction.Rollback();
                        return false;
                    }

                    job.State = SourceCleanupJobState.Importing;
                    job.FileKey = string.Empty;
                    job.AttemptCount = 0;
                    job.NextAttemptUtc = reservationUntilUtc;
                    job.LeaseUntilUtc = null;
                    job.UpdatedAtUtc = DateTime.UtcNow;
                    job.CreatedAtUtc = job.CreatedAtUtc == default(DateTime) ? job.UpdatedAtUtc : job.CreatedAtUtc;

                    using (var insertCommand = _connection.CreateCommand())
                    {
                        insertCommand.Transaction = transaction;
                        insertCommand.CommandText = @"
INSERT OR IGNORE INTO source_cleanup_jobs
 (watcher_id, tenant_id, source_path, fingerprint, file_key, import_operation_id, action, move_target_path,
 failure_directory, max_attempts, retry_initial_delay_ticks, retry_max_delay_ticks,
 attempt_count, state, next_attempt_utc, last_error, created_at_utc, updated_at_utc, lease_until_utc)
 VALUES ($watcher_id, $tenant_id, $source_path, $fingerprint, $file_key, $import_operation_id, $action, $move_target_path,
 $failure_directory, $max_attempts, $retry_initial_delay_ticks, $retry_max_delay_ticks,
 $attempt_count, $state, $next_attempt_utc, $last_error, $created_at_utc, $updated_at_utc, $lease_until_utc);";
                        Bind(insertCommand, job);
                        if (insertCommand.ExecuteNonQuery() != 1)
                        {
                            transaction.Rollback();
                            return false;
                        }
                    }

                    using (var idCommand = _connection.CreateCommand())
                    {
                        idCommand.Transaction = transaction;
                        idCommand.CommandText = "SELECT last_insert_rowid();";
                        job.Id = Convert.ToInt64(idCommand.ExecuteScalar(), CultureInfo.InvariantCulture);
                    }

                    transaction.Commit();
                    return true;
                }
            }
        }

        private int GetActiveCount(CancellationToken ct)
        {
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "SELECT COUNT(*) FROM source_cleanup_jobs;";
                    return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
                }
            }
        }

        private IReadOnlyList<SourceCleanupJob> GetDue(
            DateTime nowUtc,
            int limit,
            DateTime? staleImportCutoffUtc,
            CancellationToken ct)
        {
            var jobs = new List<SourceCleanupJob>();
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"SELECT * FROM source_cleanup_jobs
WHERE (lease_until_utc IS NULL OR lease_until_utc <= $now)
  AND (
      (action <> $keep
       AND (next_attempt_utc IS NULL OR next_attempt_utc <= $now)
       AND state IN ($pending, $retrying, $move_pending))
      OR (state = $importing AND updated_at_utc <= $stale_import_cutoff)
  )
ORDER BY updated_at_utc, id LIMIT $limit";
                    command.Parameters.AddWithValue("$now", Format(nowUtc));
                    command.Parameters.AddWithValue("$pending", (int)SourceCleanupJobState.Pending);
                    command.Parameters.AddWithValue("$retrying", (int)SourceCleanupJobState.Retrying);
                    command.Parameters.AddWithValue("$move_pending", (int)SourceCleanupJobState.MovePending);
                    command.Parameters.AddWithValue("$importing", (int)SourceCleanupJobState.Importing);
                    command.Parameters.AddWithValue("$keep", (int)SourceCleanupJobAction.Keep);
                    command.Parameters.AddWithValue(
                        "$stale_import_cutoff",
                        (object?)(staleImportCutoffUtc.HasValue ? Format(staleImportCutoffUtc.Value) : null) ?? DBNull.Value);
                    command.Parameters.AddWithValue("$limit", Math.Max(1, limit));
                    using (var reader = command.ExecuteReader())
                        while (reader.Read())
                            jobs.Add(ReadJob(reader));
                }
            }
            return jobs;
        }

        private bool TryClaim(long id, DateTime nowUtc, DateTime leaseUntilUtc, CancellationToken ct)
        {
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"UPDATE source_cleanup_jobs
SET lease_until_utc = $lease_until, updated_at_utc = $now
WHERE id = $id AND (lease_until_utc IS NULL OR lease_until_utc <= $now)";
                    command.Parameters.AddWithValue("$id", id);
                    command.Parameters.AddWithValue("$now", Format(nowUtc));
                    command.Parameters.AddWithValue("$lease_until", Format(leaseUntilUtc));
                    return command.ExecuteNonQuery() == 1;
                }
            }
        }

        private void Update(SourceCleanupJob job, CancellationToken ct)
        {
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"UPDATE source_cleanup_jobs SET
 file_key = $file_key, import_operation_id = $import_operation_id,
 attempt_count = $attempt_count, state = $state, next_attempt_utc = $next_attempt_utc,
last_error = $last_error, updated_at_utc = $updated_at_utc, lease_until_utc = NULL,
move_target_path = $move_target_path WHERE id = $id";
                    command.Parameters.AddWithValue("$id", job.Id);
                    command.Parameters.AddWithValue("$file_key", job.FileKey);
                    command.Parameters.AddWithValue("$import_operation_id", (object?)job.ImportOperationId ?? DBNull.Value);
                    command.Parameters.AddWithValue("$attempt_count", job.AttemptCount);
                    command.Parameters.AddWithValue("$state", (int)job.State);
                    command.Parameters.AddWithValue("$next_attempt_utc", (object?)FormatNullable(job.NextAttemptUtc) ?? DBNull.Value);
                    command.Parameters.AddWithValue("$last_error", (object?)job.LastError ?? DBNull.Value);
                    command.Parameters.AddWithValue("$updated_at_utc", Format(job.UpdatedAtUtc));
                    command.Parameters.AddWithValue("$move_target_path", (object?)job.MoveTargetPath ?? DBNull.Value);
                    command.ExecuteNonQuery();
                }
            }
        }

        private void Remove(long id, CancellationToken ct)
        {
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM source_cleanup_jobs WHERE id = $id";
                    command.Parameters.AddWithValue("$id", id);
                    command.ExecuteNonQuery();
                }
            }
        }

        private int PruneTerminal(DateTime cutoffUtc, int limit, CancellationToken ct)
        {
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = @"DELETE FROM source_cleanup_jobs
WHERE id IN (
    SELECT id FROM source_cleanup_jobs
    WHERE updated_at_utc <= $cutoff
      AND (action = $keep OR state = $failed)
    ORDER BY updated_at_utc, id
    LIMIT $limit
);";
                    command.Parameters.AddWithValue("$cutoff", Format(cutoffUtc));
                    command.Parameters.AddWithValue("$keep", (int)SourceCleanupJobAction.Keep);
                    command.Parameters.AddWithValue("$failed", (int)SourceCleanupJobState.Failed);
                    command.Parameters.AddWithValue("$limit", Math.Max(1, limit));
                    return command.ExecuteNonQuery();
                }
            }
        }

        private (long SizeBefore, long SizeAfter) Optimize(CancellationToken ct)
        {
            lock (_gate)
            {
                ct.ThrowIfCancellationRequested();
                var sizeBefore = File.Exists(_databasePath) ? new System.IO.FileInfo(_databasePath).Length : 0L;
                using (var command = _connection.CreateCommand())
                {
                    command.CommandText = "VACUUM;";
                    command.ExecuteNonQuery();
                }

                var sizeAfter = File.Exists(_databasePath) ? new System.IO.FileInfo(_databasePath).Length : 0L;
                return (sizeBefore, sizeAfter);
            }
        }

        private static void Bind(SqliteCommand command, SourceCleanupJob job)
        {
            command.Parameters.AddWithValue("$watcher_id", job.WatcherId);
            command.Parameters.AddWithValue("$tenant_id", job.TenantId);
            command.Parameters.AddWithValue("$source_path", job.SourcePath);
            command.Parameters.AddWithValue("$fingerprint", job.Fingerprint);
            command.Parameters.AddWithValue("$file_key", job.FileKey);
            command.Parameters.AddWithValue("$import_operation_id", (object?)job.ImportOperationId ?? DBNull.Value);
            command.Parameters.AddWithValue("$action", (int)job.Action);
            command.Parameters.AddWithValue("$move_target_path", (object?)job.MoveTargetPath ?? DBNull.Value);
            command.Parameters.AddWithValue("$failure_directory", (object?)job.FailureDirectory ?? DBNull.Value);
            command.Parameters.AddWithValue("$max_attempts", Math.Max(1, job.MaxAttempts));
            command.Parameters.AddWithValue("$retry_initial_delay_ticks", job.RetryInitialDelay.Ticks);
            command.Parameters.AddWithValue("$retry_max_delay_ticks", job.RetryMaxDelay.Ticks);
            command.Parameters.AddWithValue("$attempt_count", job.AttemptCount);
            command.Parameters.AddWithValue("$state", (int)job.State);
            command.Parameters.AddWithValue("$next_attempt_utc", (object?)FormatNullable(job.NextAttemptUtc) ?? DBNull.Value);
            command.Parameters.AddWithValue("$last_error", (object?)job.LastError ?? DBNull.Value);
            command.Parameters.AddWithValue("$created_at_utc", Format(job.CreatedAtUtc));
            command.Parameters.AddWithValue("$updated_at_utc", Format(job.UpdatedAtUtc));
            command.Parameters.AddWithValue("$lease_until_utc", (object?)FormatNullable(job.LeaseUntilUtc) ?? DBNull.Value);
        }

        private static SourceCleanupJob ReadJob(SqliteDataReader reader)
        {
            return new SourceCleanupJob
            {
                Id = reader.GetInt64(reader.GetOrdinal("id")),
                WatcherId = reader.GetString(reader.GetOrdinal("watcher_id")),
                TenantId = reader.GetString(reader.GetOrdinal("tenant_id")),
                SourcePath = reader.GetString(reader.GetOrdinal("source_path")),
                Fingerprint = reader.GetString(reader.GetOrdinal("fingerprint")),
                FileKey = reader.GetString(reader.GetOrdinal("file_key")),
                ImportOperationId = ReadNullableString(reader, "import_operation_id"),
                Action = (SourceCleanupJobAction)reader.GetInt32(reader.GetOrdinal("action")),
                MoveTargetPath = ReadNullableString(reader, "move_target_path"),
                FailureDirectory = ReadNullableString(reader, "failure_directory"),
                MaxAttempts = reader.GetInt32(reader.GetOrdinal("max_attempts")),
                RetryInitialDelay = TimeSpan.FromTicks(reader.GetInt64(reader.GetOrdinal("retry_initial_delay_ticks"))),
                RetryMaxDelay = TimeSpan.FromTicks(reader.GetInt64(reader.GetOrdinal("retry_max_delay_ticks"))),
                AttemptCount = reader.GetInt32(reader.GetOrdinal("attempt_count")),
                State = (SourceCleanupJobState)reader.GetInt32(reader.GetOrdinal("state")),
                NextAttemptUtc = ReadNullableDateTime(reader, "next_attempt_utc"),
                LastError = ReadNullableString(reader, "last_error"),
                CreatedAtUtc = ReadDateTime(reader, "created_at_utc"),
                UpdatedAtUtc = ReadDateTime(reader, "updated_at_utc"),
                LeaseUntilUtc = ReadNullableDateTime(reader, "lease_until_utc")
            };
        }

        private static string? ReadNullableString(SqliteDataReader reader, string name)
        {
            var value = reader[name];
            return value == DBNull.Value ? null : (string)value;
        }

        private static DateTime ReadDateTime(SqliteDataReader reader, string name)
        {
            return DateTime.Parse((string)reader[name], CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        private static DateTime? ReadNullableDateTime(SqliteDataReader reader, string name)
        {
            var value = reader[name];
            return value == DBNull.Value
                ? (DateTime?)null
                : DateTime.Parse((string)value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
        }

        private static string Format(DateTime value) => value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

        private static string? FormatNullable(DateTime? value) => value.HasValue ? Format(value.Value) : null;

        /// <summary>Releases the SQLite connection used by the cleanup store.</summary>
        public void Dispose()
        {
            lock (_gate)
                _connection.Dispose();
        }
    }
}
