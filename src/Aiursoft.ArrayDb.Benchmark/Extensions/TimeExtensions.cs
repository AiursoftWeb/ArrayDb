using System.Diagnostics;
using Aiursoft.ArrayDb.Benchmark.Models;
using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Benchmark.Extensions;

public enum ActionBeforeTestings
{
    None,
    Insert1KItemsBeforeTesting,
    Insert1MItemsBeforeTesting
}

public static class TimeExtensions
{
    public static async Task<TimeSpan> RunTest(TestTarget testTarget, Action<IObjectBucket<TestEntity>> action, ActionBeforeTestings actionBefore = ActionBeforeTestings.None)
    {
        // Warm up twice
        _ = await RunOneTest(testTarget, action, actionBefore);
        _ = await RunOneTest(testTarget, action, actionBefore);
        
        // Test 3 times, get average
        var testResult1 = await RunOneTest(testTarget, action, actionBefore);
        var testResult2 = await RunOneTest(testTarget, action, actionBefore);
        var testResult3 = await RunOneTest(testTarget, action, actionBefore);
        var average = new[]
        {
            testResult1,
            testResult2,
            testResult3
        }.Average(t => t.TotalMilliseconds);
        
        // Return average
        return TimeSpan.FromMilliseconds(average);
    }
    
    private static async Task<TimeSpan> RunOneTest(TestTarget testTarget, Action<IObjectBucket<TestEntity>> action, ActionBeforeTestings actionBefore = ActionBeforeTestings.None)
    {
        // get
        var target = testTarget.TestEntities();
        
        // Prepare
        switch (actionBefore)
        {
            case ActionBeforeTestings.Insert1KItemsBeforeTesting:
            {
                var dataToAdd = TestEntityFactory.CreateSome(Program.OneKilo);
                target.Add(dataToAdd);
                break;
            }
            case ActionBeforeTestings.Insert1MItemsBeforeTesting:
            {
                var dataToAdd = TestEntityFactory.CreateSome(Program.OneMillion);
                target.Add(dataToAdd);
                break;
            }
        }
        
        // Test
        var sw = new Stopwatch();
        sw.Start();
        action(target);
        sw.Stop();
        
        // Clean
        await target.DeleteAsync();
        await Task.Delay(2000);
        return sw.Elapsed;
    }
}