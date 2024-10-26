using System.Diagnostics;

namespace Aiursoft.ArrayDb.Extensions;

public static class ToolkitExtensions
{
    // ReSharper disable once FieldCanBeMadeReadOnly.Global
    public static bool ShowBench = true;
    
    public static TimeSpan RunWithTimedBench(string actionName, Action action)
    {
        if (!ShowBench)
        {
            action();
            return TimeSpan.Zero;
        }
        
        if (string.IsNullOrWhiteSpace(actionName))
        {
            actionName = "Anonymous action";
        }
        
        var stopwatch = new Stopwatch();
        stopwatch.Start();
        action();
        stopwatch.Stop();
        Console.WriteLine($"{actionName} took {stopwatch.ElapsedMilliseconds} ms.");
        return stopwatch.Elapsed;
    }
}