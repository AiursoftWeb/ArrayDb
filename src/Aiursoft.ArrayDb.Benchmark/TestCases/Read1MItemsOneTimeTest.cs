using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Read1MItemsOneTimeTest : ITestCase
{
    public string TestCaseName => "Read 1 time with 1M items";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneMillion);
        target.TestEntities.Add(dataToAdd);

        var result = Array.Empty<TestEntity>();
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            result = target.TestEntities.ReadBulk(0, Program.OneMillion);
            return Task.CompletedTask;
        });
        
        result.EnsureCorrectness(Program.OneMillion);

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = null,
            SerialRunTime = serialRunTime
        };
    }
}