using System.Diagnostics;

namespace Aiursoft.ArrayDb.Benchmark.Extensions;

public static class TimeExtensions
{
    public static async Task<TimeSpan> RunWithWatch(Func<Task> action)
    {
        var sw = new Stopwatch();
        sw.Start();
        await action();
        sw.Stop();
        return sw.Elapsed;
    }
}