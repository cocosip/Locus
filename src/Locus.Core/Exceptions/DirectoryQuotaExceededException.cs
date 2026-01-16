using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when a directory file count quota is exceeded.
    /// </summary>
    public class DirectoryQuotaExceededException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaExceededException"/> class.
        /// </summary>
        public DirectoryQuotaExceededException()
            : base("The directory file count quota has been exceeded.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaExceededException"/> class with directory details.
        /// </summary>
        /// <param name="directoryPath">The directory path.</param>
        /// <param name="currentCount">The current file count.</param>
        /// <param name="maxCount">The maximum allowed file count.</param>
        public DirectoryQuotaExceededException(string directoryPath, int currentCount, int maxCount)
            : base($"The directory '{directoryPath}' has reached its quota limit. Current: {currentCount}, Maximum: {maxCount}.")
        {
            DirectoryPath = directoryPath;
            CurrentCount = currentCount;
            MaxCount = maxCount;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DirectoryQuotaExceededException"/> class with a custom message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="directoryPath">The directory path.</param>
        public DirectoryQuotaExceededException(string message, string directoryPath)
            : base(message)
        {
            DirectoryPath = directoryPath;
        }

        /// <summary>
        /// Gets the directory path associated with this exception.
        /// </summary>
        public string? DirectoryPath { get; }

        /// <summary>
        /// Gets the current file count in the directory.
        /// </summary>
        public int CurrentCount { get; }

        /// <summary>
        /// Gets the maximum allowed file count for the directory.
        /// </summary>
        public int MaxCount { get; }
    }
}
