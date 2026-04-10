namespace Locus.Storage
{
    /// <summary>
    /// Defines the on-disk encoding used by queue journals.
    /// </summary>
    public enum JournalFormat
    {
        /// <summary>
        /// Stores one JSON payload per line.
        /// </summary>
        JsonLines = 0,

        /// <summary>
        /// Stores length-prefixed binary records with CRC protection.
        /// </summary>
        BinaryV1 = 1,
    }
}
