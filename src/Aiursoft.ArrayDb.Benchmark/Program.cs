using Aiursoft.ArrayDb.Benchmark.Abstractions;
using Aiursoft.ArrayDb.Benchmark.Extensions;
using Aiursoft.ArrayDb.Benchmark.Models;
using Aiursoft.ArrayDb.Benchmark.TestCases;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.WriteBuffer;

namespace Aiursoft.ArrayDb.Benchmark;

public abstract class Program
{
    private const string StructureFileName = "test.bin";
    private const string StructureStringsFileName = "test-strings.bin";
    public const int OneMillion = 100 * 100 * 100;
    public const int OneKilo = 1000;

    public static async Task Main()
    {
        await Clean();
        var testItems = new[]
        {
            new TestTarget
            {
                TestTargetName = "Bucket",
                TestEntities = () => new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName)
            },
            new TestTarget
            {
                TestTargetName = "Buf Bucket",
                TestEntities = () => new BufferedObjectBuckets<TestEntity>(
                    new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName)
                )
            },
            new TestTarget
            {
                TestTargetName = "BufBuf Bucket",
                TestEntities = () => new BufferedObjectBuckets<TestEntity>(
                    new BufferedObjectBuckets<TestEntity>(
                        new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName)
                    )
                )
            },
            new TestTarget
            {
                TestTargetName = "BufBufBuf Bucket",
                TestEntities = () => new BufferedObjectBuckets<TestEntity>(
                    new BufferedObjectBuckets<TestEntity>(
                        new ObjectBucket<TestEntity>(StructureFileName, StructureStringsFileName)
                    )
                )
            }
        };
        var testCases = new ITestCase[]
        {
            new Add1MItemsOneTimeTest(),
            new Add1KItems1KTimesTest(),
            new Add1MTimesTest(),
            new Read1MItemsOneTimeTest(),
            new Read1KItems1KTimesTest(),
            new Read1Item1MTimesTest(),
            new Write7Read3With1000TimesTest(),
            new Write3Read7With1000TimesTest()
        };

        Console.WriteLine("Starting benchmarking...\n\n");

        // We need to build a table to show the results of the test cases.
        // |             | OB | BOB | BBOB | POB |
        // |-------------|----|-----|------|-----|
        // | Test Case 1 |    |     |      |     |
        // | Test Case 2 |    |     |      |     |
        // | Test Case 3 |    |     |      |     |
        // | Test Case 4 |    |     |      |     |

        var finalResult = new List<TestCaseTestResults>();
        foreach (var testCase in testCases)
        {
            var testResults = new List<TestResult>();
            foreach (var testItem in testItems)
            {
                try
                {
                    Console.WriteLine($"Running test case {testCase.TestCaseName} with {testItem.TestTargetName}...");
                    var result = await testCase.RunAsync(testItem);
                    testResults.Add(result);

                    await Clean();
                    await Task.Delay(2000); // Wait for the system to cool down
                }
                catch (Exception e)
                {
                    Console.WriteLine($"The test case {testCase.TestCaseName} failed with {testItem.TestTargetName}. Error: {e.Message}");
                    throw;
                }
            }

            finalResult.Add(new TestCaseTestResults
            {
                TestCase = testCase,
                TestResults = testResults
            });
        }

        Console.WriteLine(MarkdownTableExtensions.ToMarkdownTable(finalResult));
        await Clean();
    }

    private static async Task Clean()
    {
        if (File.Exists(StructureFileName))
        {
            File.Delete(StructureFileName);
            GC.Collect();
            do
            {
                await Task.Delay(100);
            }
            while (File.Exists(StructureFileName));
        }

        if (File.Exists(StructureStringsFileName))
        {
            File.Delete(StructureStringsFileName);
            GC.Collect();
            do
            {
                await Task.Delay(100);
            }
            while (File.Exists(StructureStringsFileName));
        }
    }
}
