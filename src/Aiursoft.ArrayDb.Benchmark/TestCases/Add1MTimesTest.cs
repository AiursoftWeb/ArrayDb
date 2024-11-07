using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Add1MTimesTest : ITestCase
{
    public string TestCaseName => "Add 1M times with 1 item";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var ob = target.TestEntities;
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneMillion);
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            foreach (var data in dataToAdd)
            {
                ob.Add(data);
            }

            return Task.CompletedTask;
        });

        var parallelRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            Parallel.ForEach(dataToAdd, data => { ob.Add(data); });

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