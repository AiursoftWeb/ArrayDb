using System.Collections.Concurrent;
using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Read1Item1MTimesTest : ITestCase
{
    public string TestCaseName => "Read 1 item 1M times";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneKilo);
        target.TestEntities.Add(dataToAdd);

        var result = new List<TestEntity>();
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            for (var i = 0; i < Program.OneMillion; i++)
            {
                result.Add(target.TestEntities.Read(i));
            }

            return Task.CompletedTask;
        });
        
        result.ToArray().EnsureCorrectness(Program.OneMillion);

        var result2 = new ConcurrentBag<TestEntity>();
        var parallelRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            Parallel.For(0, Program.OneMillion, i =>
            {
                result2.Add(target.TestEntities.Read(i));
            });

            return Task.CompletedTask;
        });
        
        result2.ToArray().EnsureCorrectness(Program.OneMillion, true);

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = parallelRunTime,
            SerialRunTime = serialRunTime
        };
    }
}