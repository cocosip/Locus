using System;

namespace Locus.Storage
{
    /// <summary>
    /// Provides tenant-scoped mutual exclusion for timed-out Processing recovery.
    /// </summary>
    public interface IProcessingTimeoutRecoveryCoordinator
    {
        /// <summary>
        /// Tries to enter timed-out recovery for the specified tenant.
        /// </summary>
        bool TryEnter(string tenantId, out IDisposable? releaser);
    }
}
