using System.Diagnostics;
using Aiursoft.ArrayDb.Engine;
using Aiursoft.ArrayDb.Tests.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class PerformanceTestBuffered : ArrayDbTestBase
{
    [TestMethod]
    public async Task PerformanceTestBulkWrite()
    {
        var persistService =
            new ObjectBuckets<SampleData>("sampleData.bin", "sampleDataStrings.bin");
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        for (var i = 0; i < 1000000; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            await buffer.AddAsync(sample);
        }
        stopWatch.Stop();

        Console.WriteLine($"Write 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Console.WriteLine(persistService.OutputStatistics());
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 5);
    }
}