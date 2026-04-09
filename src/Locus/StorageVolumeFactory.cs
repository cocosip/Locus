using System;
using System.IO.Abstractions;
using Locus.Core.Abstractions;
using Locus.FileSystem;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Locus
{
    /// <summary>
    /// Creates configured storage volume instances from runtime options.
    /// </summary>
    internal static class StorageVolumeFactory
    {
        public static IStorageVolume CreateVolume(
            VolumeConfiguration config,
            IFileSystem fileSystem,
            IServiceProvider serviceProvider)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            if (fileSystem == null)
                throw new ArgumentNullException(nameof(fileSystem));
            if (serviceProvider == null)
                throw new ArgumentNullException(nameof(serviceProvider));

            switch (config.VolumeType.ToLowerInvariant())
            {
                case "localfilesystem":
                case "local":
                {
                    var logger = serviceProvider.GetRequiredService<ILogger<LocalFileSystemVolume>>();
                    return new LocalFileSystemVolume(
                        fileSystem,
                        logger,
                        config.VolumeId,
                        config.MountPath,
                        config.ShardingDepth,
                        writeBufferSize: config.WriteBufferSize,
                        copyBufferSize: config.CopyBufferSize,
                        forceFlushAfterWrite: config.ForceFlushAfterWrite);
                }

                default:
                    throw new NotSupportedException($"Volume type '{config.VolumeType}' is not supported");
            }
        }
    }
}
