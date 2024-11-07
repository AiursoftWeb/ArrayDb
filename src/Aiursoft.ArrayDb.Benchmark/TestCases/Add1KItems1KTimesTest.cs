using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Add1KItems1KTimesTest : ITestCase
{
    public string TestCaseName => "Add 1K items 1K times";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneKilo);
        var serialRunTime = await TimeExtensions.RunTest(target, t =>
        {
            for (var i = 0; i < Program.OneKilo; i++)
            {
                t.Add(dataToAdd);
            }
        });
        var parallelRunTime = await TimeExtensions.RunTest(target, t =>
        {
            Parallel.For(0, Program.OneKilo, _ =>
            {
                t.Add(dataToAdd);
            });
        });

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = parallelRunTime,
            SerialRunTime = serialRunTime
        };
    }
}