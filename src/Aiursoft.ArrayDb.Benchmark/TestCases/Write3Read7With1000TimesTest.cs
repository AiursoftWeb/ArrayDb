using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.TestCases;

public class Write3Read7With1000TimesTest : ITestCase
{
    public string TestCaseName => "Write 3 read 7 with 1000 items";

    public async Task<TestResult> RunAsync(TestTarget target)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneKilo);
        var serialRunTime = await TimeExtensions.RunTest(target, t =>
        {
            for (var i = 0; i < Program.OneKilo; i++)
            {
                if (new Random().NextDouble() < 0.3)
                {
                    // Add OneKilo items
                    t.Add(dataToAdd);
                }
                else
                {
                    // Randomly read OneKilo items
                    var index = new Random().Next(0, t.Count - Program.OneKilo);
                    t.ReadBulk(index, Program.OneKilo);
                }
            }
        }, actionBefore: ActionBeforeTestings.Insert1KItemsBeforeTesting);
        
        var parallelRunTime = await TimeExtensions.RunTest(target, t =>
        {
            Parallel.For(0, Program.OneKilo, _ =>
            {
                if (new Random().NextDouble() < 0.3)
                {
                    // Add OneKilo items
                    t.Add(dataToAdd);
                }
                else
                {
                    // Randomly read OneKilo items
                    var index = new Random().Next(0, t.Count - Program.OneKilo);
                    t.ReadBulk(index, Program.OneKilo);
                }
            });
        }, actionBefore: ActionBeforeTestings.Insert1KItemsBeforeTesting);

        return new TestResult
        {
            TestedItem = target.TestTargetName,
            ParallelRunTime = parallelRunTime,
            SerialRunTime = serialRunTime
        };
    }
}