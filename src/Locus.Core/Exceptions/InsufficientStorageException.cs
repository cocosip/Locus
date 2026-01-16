using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when there is insufficient storage space available.
    /// </summary>
    public class InsufficientStorageException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InsufficientStorageException"/> class.
        /// </summary>
        public InsufficientStorageException()
            : base("Insufficient storage space is available.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InsufficientStorageException"/> class with space details.
        /// </summary>
        /// <param name="requiredSpace">The required space in bytes.</param>
        /// <param name="availableSpace">The available space in bytes.</param>
        public InsufficientStorageException(long requiredSpace, long availableSpace)
            : base($"Insufficient storage space. Required: {requiredSpace} bytes, Available: {availableSpace} bytes.")
        {
            RequiredSpace = requiredSpace;
            AvailableSpace = availableSpace;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InsufficientStorageException"/> class with a custom message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public InsufficientStorageException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Gets the required storage space in bytes.
        /// </summary>
        public long RequiredSpace { get; }

        /// <summary>
        /// Gets the available storage space in bytes.
        /// </summary>
        public long AvailableSpace { get; }
    }
}
