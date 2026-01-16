using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when attempting to process a file that is already being processed.
    /// </summary>
    public class FileAlreadyProcessingException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="FileAlreadyProcessingException"/> class.
        /// </summary>
        public FileAlreadyProcessingException()
            : base("The file is already being processed.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileAlreadyProcessingException"/> class with a file key.
        /// </summary>
        /// <param name="fileKey">The file key.</param>
        public FileAlreadyProcessingException(string fileKey)
            : base($"The file '{fileKey}' is already being processed.")
        {
            FileKey = fileKey;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FileAlreadyProcessingException"/> class with a custom message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="fileKey">The file key.</param>
        public FileAlreadyProcessingException(string message, string fileKey)
            : base(message)
        {
            FileKey = fileKey;
        }

        /// <summary>
        /// Gets the file key associated with this exception.
        /// </summary>
        public string? FileKey { get; }
    }
}
