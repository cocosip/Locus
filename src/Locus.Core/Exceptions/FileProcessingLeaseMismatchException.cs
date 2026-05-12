using System;
using Locus.Core.Models;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when a worker tries to complete or fail a file using a stale processing lease.
    /// </summary>
    public class FileProcessingLeaseMismatchException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FileProcessingLeaseMismatchException"/> class.
        /// </summary>
        /// <param name="fileKey">The file key.</param>
        /// <param name="expectedProcessingStartTimeUtc">The processing start time expected by the caller.</param>
        /// <param name="actualProcessingStartTimeUtc">The current processing start time, if one is still active.</param>
        public FileProcessingLeaseMismatchException(
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            DateTime? actualProcessingStartTimeUtc = null)
            : this(fileKey, expectedProcessingStartTimeUtc, actualProcessingStartTimeUtc, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileProcessingLeaseMismatchException"/> class.
        /// </summary>
        /// <param name="fileKey">The file key.</param>
        /// <param name="expectedProcessingStartTimeUtc">The processing start time expected by the caller.</param>
        /// <param name="actualProcessingStartTimeUtc">The current processing start time, if one is still active.</param>
        /// <param name="actualStatus">The current metadata status, if available.</param>
        public FileProcessingLeaseMismatchException(
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            DateTime? actualProcessingStartTimeUtc,
            FileProcessingStatus? actualStatus)
            : base(CreateMessage(fileKey, expectedProcessingStartTimeUtc, actualProcessingStartTimeUtc, actualStatus))
        {
            FileKey = fileKey;
            ExpectedProcessingStartTimeUtc = expectedProcessingStartTimeUtc;
            ActualProcessingStartTimeUtc = actualProcessingStartTimeUtc;
            ActualStatus = actualStatus;
        }

        /// <summary>
        /// Gets the file key associated with this exception.
        /// </summary>
        public string FileKey { get; }

        /// <summary>
        /// Gets the processing start time expected by the caller.
        /// </summary>
        public DateTime ExpectedProcessingStartTimeUtc { get; }

        /// <summary>
        /// Gets the current processing start time if one is still active.
        /// </summary>
        public DateTime? ActualProcessingStartTimeUtc { get; }

        /// <summary>
        /// Gets the current metadata status if known.
        /// </summary>
        public FileProcessingStatus? ActualStatus { get; }

        private static string CreateMessage(
            string fileKey,
            DateTime expectedProcessingStartTimeUtc,
            DateTime? actualProcessingStartTimeUtc,
            FileProcessingStatus? actualStatus)
        {
            if (actualProcessingStartTimeUtc.HasValue)
            {
                return $"The processing lease for file '{fileKey}' no longer matches. Expected lease started at " +
                       $"{expectedProcessingStartTimeUtc:O}, but the active lease started at {actualProcessingStartTimeUtc.Value:O}. " +
                       $"Actual status: {actualStatus?.ToString() ?? "Unknown"}.";
            }

            return $"The processing lease for file '{fileKey}' no longer matches the active metadata state. " +
                   $"Expected lease started at {expectedProcessingStartTimeUtc:O}. " +
                   $"Actual status: {actualStatus?.ToString() ?? "Unknown"}.";
        }
    }
}
