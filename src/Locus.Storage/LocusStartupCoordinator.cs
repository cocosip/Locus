using System;
using System.Threading;
using System.Threading.Tasks;

namespace Locus.Storage
{
    /// <summary>
    /// Coordinates startup prerequisites that background workers must not outrun.
    /// </summary>
    public sealed class LocusStartupCoordinator
    {
        private readonly TaskCompletionSource<object?> _volumesReady =
            new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<object?> _tenantsReady =
            new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<object?> _databaseHealthReady =
            new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>
        /// A coordinator that is already ready. Useful for direct construction in tests and samples.
        /// </summary>
        public static LocusStartupCoordinator Ready { get; } = CreateReady();

        /// <summary>
        /// Completes when storage volumes have been mounted and registered.
        /// </summary>
        public Task VolumesReady => _volumesReady.Task;

        /// <summary>
        /// Completes when configured tenants and quotas have been initialized.
        /// </summary>
        public Task TenantsReady => _tenantsReady.Task;

        /// <summary>
        /// Completes when startup database health checks and recovery have finished.
        /// </summary>
        public Task DatabaseHealthReady => _databaseHealthReady.Task;

        /// <summary>
        /// Completes when all Locus runtime prerequisites are ready.
        /// </summary>
        public Task RuntimeReady => Task.WhenAll(VolumesReady, TenantsReady, DatabaseHealthReady);

        /// <summary>
        /// Marks storage volume initialization as complete.
        /// </summary>
        public void MarkVolumesReady() => _volumesReady.TrySetResult(null);

        /// <summary>
        /// Marks tenant initialization as complete.
        /// </summary>
        public void MarkTenantsReady() => _tenantsReady.TrySetResult(null);

        /// <summary>
        /// Marks startup database health checks as complete.
        /// </summary>
        public void MarkDatabaseHealthReady() => _databaseHealthReady.TrySetResult(null);

        /// <summary>
        /// Fails any startup prerequisite that has not completed yet.
        /// </summary>
        public void Fail(Exception exception)
        {
            if (exception == null)
                throw new ArgumentNullException(nameof(exception));

            _volumesReady.TrySetException(exception);
            _tenantsReady.TrySetException(exception);
            _databaseHealthReady.TrySetException(exception);
        }

        /// <summary>
        /// Waits until all runtime prerequisites are ready.
        /// </summary>
        public async Task WaitForRuntimeReadyAsync(CancellationToken ct = default)
        {
            await WaitForTaskAsync(RuntimeReady, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Waits until storage volumes have been mounted and registered.
        /// </summary>
        public async Task WaitForVolumesReadyAsync(CancellationToken ct = default)
        {
            await WaitForTaskAsync(VolumesReady, ct).ConfigureAwait(false);
        }

        private static async Task WaitForTaskAsync(Task task, CancellationToken ct)
        {
            if (task.IsCompleted)
            {
                await task.ConfigureAwait(false);
                return;
            }

            if (!ct.CanBeCanceled)
            {
                await task.ConfigureAwait(false);
                return;
            }

            var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (ct.Register(
                state => ((TaskCompletionSource<object?>)state!).TrySetCanceled(),
                tcs))
            {
                var completed = await Task.WhenAny(task, tcs.Task).ConfigureAwait(false);
                await completed.ConfigureAwait(false);
                await task.ConfigureAwait(false);
            }
        }

        private static LocusStartupCoordinator CreateReady()
        {
            var coordinator = new LocusStartupCoordinator();
            coordinator.MarkVolumesReady();
            coordinator.MarkTenantsReady();
            coordinator.MarkDatabaseHealthReady();
            return coordinator;
        }
    }
}
