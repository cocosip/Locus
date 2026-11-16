using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Locus.Storage.Tests
{
    /// <summary>
    /// Replaces fixed "wait for the file handle to be released" sleeps with a bounded poll.
    /// </summary>
    internal static class TestFileWaits
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Waits until the database file can be opened for exclusive write access, which means
        /// no connection or other handle is still holding it, then fails with a clear message.
        /// </summary>
        public static async Task ReleaseFileHandlesAsync(string dbPath)
        {
            var stopwatch = Stopwatch.StartNew();
            var delay = TimeSpan.FromMilliseconds(10);

            while (!CanOpenExclusive(dbPath))
            {
                if (stopwatch.Elapsed >= Timeout)
                {
                    throw new TimeoutException(
                        $"The database file '{dbPath}' was still locked after {Timeout.TotalSeconds:0.#}s; " +
                        "a connection or file handle was not released.");
                }

                await Task.Delay(delay).ConfigureAwait(false);
                delay = TimeSpan.FromMilliseconds(Math.Min(delay.TotalMilliseconds * 2, MaxDelay.TotalMilliseconds));
            }
        }

        private static bool CanOpenExclusive(string dbPath)
        {
            try
            {
                using (new FileStream(dbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                    return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
        }
    }
}
