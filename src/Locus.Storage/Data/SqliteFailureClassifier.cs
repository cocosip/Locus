using System;
using System.IO;
using Microsoft.Data.Sqlite;

namespace Locus.Storage.Data
{
    /// <summary>
    /// Classifies SQLite failures so that transient I/O problems are never mistaken for
    /// database corruption. A false corruption verdict deletes the tenant database and
    /// rebuilds it from the hot cache, which loses cold history; therefore only
    /// SQLITE_CORRUPT (11) and SQLITE_NOTADB (26) may be treated as corruption.
    /// </summary>
    internal static class SqliteFailureClassifier
    {
        internal static bool IsCorruption(Exception exception)
        {
            return exception is SqliteException sqliteException
                && (sqliteException.SqliteErrorCode == 11 || sqliteException.SqliteErrorCode == 26);
        }

        internal static bool IsTransient(Exception exception)
        {
            if (exception is IOException)
                return true;

            if (exception is SqliteException sqliteException)
            {
                return sqliteException.SqliteErrorCode == 5   // SQLITE_BUSY
                    || sqliteException.SqliteErrorCode == 6   // SQLITE_LOCKED
                    || sqliteException.SqliteErrorCode == 10; // SQLITE_IOERR
            }

            return false;
        }
    }
}
