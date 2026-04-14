using BenchmarkDotNet.Running;

namespace Locus.Benchmarks
{
    public class Program
    {
        public static int Main(string[] args)
        {
            if (OrphanRebuildPeakMemoryCommand.ShouldRun(args))
                return OrphanRebuildPeakMemoryCommand.RunAsync(args).GetAwaiter().GetResult();

            if (AcceptedWriteOverlapCommand.ShouldRun(args))
                return AcceptedWriteOverlapCommand.RunAsync(args).GetAwaiter().GetResult();

            if (VolumeWritePhaseBreakdownCommand.ShouldRun(args))
                return VolumeWritePhaseBreakdownCommand.RunAsync(args).GetAwaiter().GetResult();

            if (DirectVolumeBreakdownCommand.ShouldRun(args))
                return DirectVolumeBreakdownCommand.RunAsync(args).GetAwaiter().GetResult();

            if (WritePathBreakdownCommand.ShouldRun(args))
                return WritePathBreakdownCommand.RunAsync(args).GetAwaiter().GetResult();

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
            return 0;
        }
    }
}
