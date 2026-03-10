using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Locus.Core.Abstractions;

namespace Locus.Storage
{
    /// <summary>
    /// Shared registry of mounted storage volumes.
    /// </summary>
    public sealed class StorageVolumeRegistry
    {
        private readonly ConcurrentDictionary<string, IStorageVolume> _volumes =
            new ConcurrentDictionary<string, IStorageVolume>(StringComparer.Ordinal);

        /// <summary>
        /// Registers a mounted volume.
        /// </summary>
        public void Register(IStorageVolume volume)
        {
            if (volume == null)
                throw new ArgumentNullException(nameof(volume));

            if (_volumes.TryGetValue(volume.VolumeId, out var existing))
            {
                if (ReferenceEquals(existing, volume))
                    return;

                throw new InvalidOperationException($"Volume {volume.VolumeId} is already registered");
            }

            if (!_volumes.TryAdd(volume.VolumeId, volume)
                && _volumes.TryGetValue(volume.VolumeId, out existing)
                && !ReferenceEquals(existing, volume))
            {
                throw new InvalidOperationException($"Volume {volume.VolumeId} is already registered");
            }
        }

        /// <summary>
        /// Tries to get a registered volume by id.
        /// </summary>
        public bool TryGetVolume(string volumeId, out IStorageVolume volume)
        {
            if (string.IsNullOrWhiteSpace(volumeId))
            {
                volume = null!;
                return false;
            }

            return _volumes.TryGetValue(volumeId, out volume!);
        }

        /// <summary>
        /// Returns a snapshot of currently registered volumes.
        /// </summary>
        public IReadOnlyCollection<IStorageVolume> GetVolumes()
        {
            return _volumes.Values as IReadOnlyCollection<IStorageVolume>
                ?? new List<IStorageVolume>(_volumes.Values);
        }
    }
}
