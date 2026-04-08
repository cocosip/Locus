namespace Locus.Core.Abstractions
{
    /// <summary>
    /// Provides an explicit health probe that bypasses any cached volume status.
    /// </summary>
    public interface IStorageVolumeHealthProbe
    {
        /// <summary>
        /// Performs an immediate health probe and updates any internal cache with the latest result.
        /// </summary>
        bool ProbeHealth();
    }
}
