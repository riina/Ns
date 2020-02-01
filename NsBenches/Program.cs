using BenchmarkDotNet.Running;

namespace NsBenches {
    class Program {
        public static void Main(string[] args) => BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}