using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Read1KItems1KTimesTest : ITestCase
{
    public string TestCaseName => "Read 1K items 1K times";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneKilo);
        target.TestEntities.Add(dataToAdd);

        var result = Array.Empty<TestEntity>();
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            for (var i = 0; i < Program.OneKilo; i++)
            {
                result = target.TestEntities.ReadBulk(0, Program.OneKilo);
            }

            return Task.CompletedTask;
        });
        
        result.EnsureCorrectness(Program.OneKilo);
        
        var parallelRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            Parallel.For(0, Program.OneKilo, _ =>
            {
                target.TestEntities.ReadBulk(0, Program.OneKilo);
            });

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