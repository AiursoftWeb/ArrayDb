using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Read1KItems1KTimesTest : ITestCase
{
    public string TestCaseName => "Read 1K items 1K times";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var serialRunTime = await TimeExtensions.RunTest(target, t =>
        {
            for (var i = 0; i < Program.OneKilo; i++)
            {
                t.ReadBulk(indexFrom: i * Program.OneKilo, take: Program.OneKilo);
            }
        }, actionBefore: ActionBeforeTestings.Insert1MItemsBeforeTesting);
        
        var parallelRunTime = await TimeExtensions.RunTest(target, t =>
        {
            Parallel.For(0, Program.OneMillion, i =>
            {
                t.ReadBulk(indexFrom: i * Program.OneKilo, take: Program.OneKilo);
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