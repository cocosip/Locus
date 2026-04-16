namespace Locus.Storage
{
    /// <summary>
    /// Represents the outcome of a timed-out Processing recovery pass.
    /// </summary>
    public readonly struct ProcessingTimeoutRecoveryResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ProcessingTimeoutRecoveryResult"/> struct.
        /// </summary>
        public ProcessingTimeoutRecoveryResult(bool gateAcquired, int candidateCount, int recoveredCount)
        {
            GateAcquired = gateAcquired;
            CandidateCount = candidateCount;
            RecoveredCount = recoveredCount;
        }

        /// <summary>
        /// Gets a value indicating whether the tenant recovery gate was acquired.
        /// </summary>
        public bool GateAcquired { get; }

        /// <summary>
        /// Gets the number of timed-out candidates loaded in this pass.
        /// </summary>
        public int CandidateCount { get; }

        /// <summary>
        /// Gets the number of files successfully reset to Pending in this pass.
        /// </summary>
        public int RecoveredCount { get; }
    }
}
