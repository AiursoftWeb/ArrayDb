using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Add1MTimesTest : ITestCase
{
    public string TestCaseName => "Add 1M times with 1 item";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneMillion);
        var serialRunTime = await TimeExtensions.RunTest(target, (t) =>
        {
            foreach (var data in dataToAdd)
            {
                t.Add(data);
            }
        });

        var parallelRunTime = await TimeExtensions.RunTest(target, t =>
        {
            Parallel.ForEach(dataToAdd, data =>
            {
                t.Add(data);
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