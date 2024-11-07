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
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            for (var i = 0; i < Program.OneKilo; i++)
            {
                target.TestEntities.Add(dataToAdd);
            }

            return Task.CompletedTask;
        });
        
        var parallelRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            Parallel.For(0, Program.OneKilo, _ => { target.TestEntities.Add(dataToAdd); });
            return Task.CompletedTask;
        });

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = parallelRunTime,
            SerialRunTime = serialRunTime
        };
    }
}