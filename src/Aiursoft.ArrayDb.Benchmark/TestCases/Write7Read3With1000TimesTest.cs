using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Write7Read3With1000TimesTest : ITestCase
{
    public string TestCaseName => "Write 7 read 3 with 1000 items";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneKilo);
        target.TestEntities.Add(dataToAdd); // Ensure at least OneKilo items in the bucket.
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            for (var i = 0; i < Program.OneKilo; i++)
            {
                if (new Random().NextDouble() < 0.7)
                {
                    // Add OneKilo items
                    target.TestEntities.Add(dataToAdd);
                }
                else
                {
                    // Randomly read OneKilo items
                    var index = new Random().Next(0, target.TestEntities.Count - Program.OneKilo);
                    target.TestEntities.ReadBulk(index, Program.OneKilo);
                }
            }

            return Task.CompletedTask;
        });
        
        var parallelRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            Parallel.For(0, Program.OneKilo, _ =>
            {
                if (new Random().NextDouble() < 0.7)
                {
                    // Add OneKilo items
                    target.TestEntities.Add(dataToAdd);
                }
                else
                {
                    // Randomly read OneKilo items
                    var index = new Random().Next(0, target.TestEntities.Count - Program.OneKilo);
                    target.TestEntities.ReadBulk(index, Program.OneKilo);
                }
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