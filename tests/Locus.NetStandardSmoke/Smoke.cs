namespace Locus.NetStandardSmoke;

/// <summary>
/// Compile-only guard that the netstandard2.0 asset resolves against its 10.x dependency group.
/// </summary>
public static class Smoke
{
    /// <summary>Gets a public type from the netstandard2.0 Locus assembly.</summary>
    public static System.Type OptionsType => typeof(Locus.LocusOptions);
}
