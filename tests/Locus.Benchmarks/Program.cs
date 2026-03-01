using BenchmarkDotNet.Running;

namespace Locus.Benchmarks
{
    public class Program
    {
        public static int Main(string[] args)
        {
            if (OrphanRebuildPeakMemoryCommand.ShouldRun(args))
                return OrphanRebuildPeakMemoryCommand.RunAsync(args).GetAwaiter().GetResult();

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
            return 0;
        }
    }
}
