using System.Diagnostics;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.WriteBuffer;

namespace Aiursoft.ArrayDb.Benchmark;

public abstract class Program
{
    private const string StructureFileName = "test.bin";
    private const string StructureStringsFileName = "test-strings.bin";
    public const int OneMillion = 100 * 100 * 100;

    public static async Task Main()
    {
        if (File.Exists(StructureFileName))
        {
            File.Delete(StructureFileName);
        }
        if (File.Exists(StructureStringsFileName))
        {
            File.Delete(StructureStringsFileName);
        }
        
        var ob = new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName);
        // ReSharper disable once InconsistentNaming
        var b_ob =
            new BufferedObjectBuckets<TestEntity>(
                new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName)
            );
        // ReSharper disable once InconsistentNaming
        var b_b_ob =
            new BufferedObjectBuckets<TestEntity>(
                new BufferedObjectBuckets<TestEntity>(
                    new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName)
                )
            );

        var testCases = new ITestCase[]
        {
            new Add1MTimesTest(),
            new Add1MItemsOneTimeTest()
        };

        Console.WriteLine("Start testing...");
        // TODO: Test each item.
        foreach (var testCase in testCases)
        {
            Console.WriteLine($"Test case: {testCase.TestCaseName}");
            var result = await testCase.RunAsync(ob);
            Console.WriteLine("======================================");
            Console.WriteLine($"Serial run time: {result.SerialRunTime}");
            if (result.ParallelRunTime.HasValue)
            {
                Console.WriteLine($"Parallel run time: {result.ParallelRunTime}");
            }
            Console.WriteLine("====================================== \n\n");
            await Task.Delay(2000); // Wait for a while to cool down for the next test.
        }
    }
}

public static class TimeExtensions
{
    public static async Task<TimeSpan> RunWithWatch(Func<Task> action)
    {
        var sw = new Stopwatch();
        sw.Start();
        await action();
        sw.Stop();
        return sw.Elapsed;
    }
}

public interface ITestCase
{
    public string TestCaseName { get; }
    public Task<TestResult> RunAsync(IObjectBucket<TestEntity> ob);
}

public class TestResult
{
    public required TimeSpan? ParallelRunTime { get; set; }

    public required TimeSpan SerialRunTime { get; set; }
}

public class Add1MTimesTest : ITestCase
{
    public string TestCaseName => "Add One Million Times Test";
    public async Task<TestResult> RunAsync(IObjectBucket<TestEntity> ob)
    {
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
            ParallelRunTime = parallelRunTime,
            SerialRunTime = serialRunTime
        };
    }
}


public class Add1MItemsOneTimeTest : ITestCase
{
    public string TestCaseName => "Add One Million Items One Time Test";
    public async Task<TestResult> RunAsync(IObjectBucket<TestEntity> ob)
    {
        var dataToAdd = TestEntityFactory.CreateSome(Program.OneMillion);
        var serialRunTime = await TimeExtensions.RunWithWatch(() =>
        {
            ob.Add(dataToAdd);

            return Task.CompletedTask;
        });

        return new TestResult
        {
            ParallelRunTime = null,
            SerialRunTime = serialRunTime
        };
    }
}