using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Locus.Storage
{
    /// <summary>
    /// Default tenant-scoped coordination for timed-out Processing recovery.
    /// </summary>
    public sealed class ProcessingTimeoutRecoveryCoordinator : IProcessingTimeoutRecoveryCoordinator
    {
        private readonly ConcurrentDictionary<string, byte> _activeTenants =
            new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);

        /// <inheritdoc/>
        public bool TryEnter(string tenantId, out IDisposable? releaser)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentException("TenantId cannot be empty", nameof(tenantId));

            if (!_activeTenants.TryAdd(tenantId, 0))
            {
                releaser = null;
                return false;
            }

            releaser = new Releaser(_activeTenants, tenantId);
            return true;
        }

        private sealed class Releaser : IDisposable
        {
            private readonly ConcurrentDictionary<string, byte> _activeTenants;
            private readonly string _tenantId;
            private int _disposed;

            public Releaser(ConcurrentDictionary<string, byte> activeTenants, string tenantId)
            {
                _activeTenants = activeTenants;
                _tenantId = tenantId;
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0)
                    return;

                _activeTenants.TryRemove(_tenantId, out _);
            }
        }
    }
}
