using System;
using Locus.Core.Models;
using Microsoft.Extensions.DependencyInjection;

namespace Locus
{
    /// <summary>
    /// Builder for configuring Locus storage system with a fluent API.
    /// </summary>
    public class LocusBuilder
    {
        private readonly IServiceCollection _services;
        private readonly LocusOptions _options;

        /// <summary>
        /// Initializes a new instance of the <see cref="LocusBuilder"/> class.
        /// </summary>
        /// <param name="services">The service collection.</param>
        public LocusBuilder(IServiceCollection services)
        {
            _services = services ?? throw new ArgumentNullException(nameof(services));
            _options = new LocusOptions();
        }

        /// <summary>
        /// Adds a local file system storage volume.
        /// </summary>
        /// <param name="volumeId">The unique volume identifier.</param>
        /// <param name="mountPath">The mount path for the volume.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder AddLocalVolume(string volumeId, string mountPath)
        {
            if (string.IsNullOrWhiteSpace(volumeId))
                throw new ArgumentException("Volume ID cannot be empty", nameof(volumeId));

            if (string.IsNullOrWhiteSpace(mountPath))
                throw new ArgumentException("Mount path cannot be empty", nameof(mountPath));

            _options.Volumes.Add(new VolumeConfiguration
            {
                VolumeId = volumeId,
                MountPath = mountPath,
                VolumeType = "LocalFileSystem"
            });

            return this;
        }

        /// <summary>
        /// Configures the metadata storage directory.
        /// </summary>
        /// <param name="directory">The directory path.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithMetadataDirectory(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentException("Directory cannot be empty", nameof(directory));

            _options.MetadataDirectory = directory;
            return this;
        }

        /// <summary>
        /// Configures the quota storage directory.
        /// </summary>
        /// <param name="directory">The directory path.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithQuotaDirectory(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentException("Directory cannot be empty", nameof(directory));

            _options.QuotaDirectory = directory;
            return this;
        }

        /// <summary>
        /// Configures the file retry policy.
        /// </summary>
        /// <param name="configure">Configuration action for retry policy.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithRetryPolicy(Action<FileRetryPolicy> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            configure(_options.RetryPolicy);
            return this;
        }

        /// <summary>
        /// Configures cleanup options.
        /// </summary>
        /// <param name="configure">Configuration action for cleanup options.</param>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder WithCleanupOptions(Action<Storage.CleanupOptions> configure)
        {
            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            configure(_options.CleanupOptions);
            return this;
        }

        /// <summary>
        /// Enables the background cleanup service.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder EnableBackgroundCleanup()
        {
            _options.EnableBackgroundCleanup = true;
            return this;
        }

        /// <summary>
        /// Disables the background cleanup service.
        /// </summary>
        /// <returns>The builder for chaining.</returns>
        public LocusBuilder DisableBackgroundCleanup()
        {
            _options.EnableBackgroundCleanup = false;
            return this;
        }

        /// <summary>
        /// Builds and registers Locus services with the service collection.
        /// </summary>
        /// <returns>The service collection for further configuration.</returns>
        public IServiceCollection Build()
        {
            _services.AddLocus(_options);
            return _services;
        }
    }

    /// <summary>
    /// Extension methods for LocusBuilder.
    /// </summary>
    public static class LocusBuilderExtensions
    {
        /// <summary>
        /// Adds Locus services using a fluent builder API.
        /// </summary>
        /// <param name="services">The service collection.</param>
        /// <param name="configure">Configuration action for the builder.</param>
        /// <returns>The service collection for chaining.</returns>
        public static IServiceCollection AddLocus(
            this IServiceCollection services,
            Action<LocusBuilder> configure)
        {
            if (services == null)
                throw new ArgumentNullException(nameof(services));

            if (configure == null)
                throw new ArgumentNullException(nameof(configure));

            var builder = new LocusBuilder(services);
            configure(builder);
            return builder.Build();
        }
    }
}
