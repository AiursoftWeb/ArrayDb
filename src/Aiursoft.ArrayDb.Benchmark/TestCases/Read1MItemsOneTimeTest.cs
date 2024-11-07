using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Read1MItemsOneTimeTest : ITestCase
{
    public string TestCaseName => "Read 1 time with 1M items";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var serialRunTime = await TimeExtensions.RunTest(target, t =>
        {
            t.ReadBulk(0, Program.OneMillion);
        }, actionBefore: ActionBeforeTestings.Insert1MItemsBeforeTesting);
        
        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = null,
            SerialRunTime = serialRunTime
        };
    }
}