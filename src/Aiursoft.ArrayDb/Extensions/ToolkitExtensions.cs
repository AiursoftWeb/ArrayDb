using System.Diagnostics;

namespace Aiursoft.ArrayDb.Extensions;

public static class ToolkitExtensions
{
    public static TimeSpan RunWithTimedBench(string actionName, Action action)
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        action();
        stopwatch.Stop();
        Console.WriteLine($"{actionName} took {stopwatch.ElapsedMilliseconds} ms.");
        return stopwatch.Elapsed;
    }
}