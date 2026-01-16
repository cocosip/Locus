using System;

namespace Locus.Core.Exceptions
{
    /// <summary>
    /// Exception thrown when no healthy storage volumes are available.
    /// </summary>
    public class StorageVolumeUnavailableException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="StorageVolumeUnavailableException"/> class.
        /// </summary>
        public StorageVolumeUnavailableException()
            : base("No healthy storage volumes are available for operations.")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageVolumeUnavailableException"/> class with a custom message.
        /// </summary>
        /// <param name="message">The exception message.</param>
        public StorageVolumeUnavailableException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="StorageVolumeUnavailableException"/> class with a custom message and inner exception.
        /// </summary>
        /// <param name="message">The exception message.</param>
        /// <param name="innerException">The inner exception.</param>
        public StorageVolumeUnavailableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
