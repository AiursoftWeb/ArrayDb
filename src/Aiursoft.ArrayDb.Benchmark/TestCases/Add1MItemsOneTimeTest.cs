using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Add1MItemsOneTimeTest : ITestCase
{
    public string TestCaseName => "Add 1 time with 1M items";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneMillion);
        var serialRunTime = await TimeExtensions.RunTest(target, t =>
        {
            t.Add(dataToAdd);
        });

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = null,
            SerialRunTime = serialRunTime
        };
    }
}