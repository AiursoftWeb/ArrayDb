using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Read1Item1MTimesTest : ITestCase
{
    public string TestCaseName => "Read 1 item 1M times";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var serialRunTime = await TimeExtensions.RunTest(target, t =>
        {
            for (var i = 0; i < Program.OneMillion; i++)
            {
                t.Read(i);
            }
        }, actionBefore: ActionBeforeTestings.Insert1MItemsBeforeTesting);
        
        var parallelRunTime = await TimeExtensions.RunTest(target, t =>
        {
            Parallel.For(0, Program.OneMillion, i =>
            {
                t.Read(i);
            });
        }, actionBefore: ActionBeforeTestings.Insert1MItemsBeforeTesting);

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = parallelRunTime,
            SerialRunTime = serialRunTime
        };
    }
}