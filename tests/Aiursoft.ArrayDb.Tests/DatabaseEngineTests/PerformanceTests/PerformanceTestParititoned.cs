using System.Diagnostics;
using Aiursoft.ArrayDb.Partitions;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests.PerformanceTests;

[TestClass]
public class PerformanceTestParititoned
{
    [TestMethod]
    public async Task TestAddPartitioned()
    {
        // Get Temp path
        var testPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
        Directory.CreateDirectory(testPath);
        var partitionedService =
            new PartitionedObjectBucket<DataCanBePartitioned, int>("my-db", testPath);
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        for (var i = 0; i < 100 * 100 * 100; i++)
        {
            var sample = new DataCanBePartitioned
            {
                Id = i,
                ThreadId = i % 10,
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        stopWatch.Stop();
        Console.WriteLine($"Time to provision 100 * 100 * 100 items: {stopWatch.ElapsedMilliseconds}ms");
        
        stopWatch.Reset();
        stopWatch.Start();
        await partitionedService.SyncAsync();
        Console.WriteLine($"Time to archive 100 * 100 * 100 items: {stopWatch.ElapsedMilliseconds}ms");
        stopWatch.Stop();
        
        var results = partitionedService.ReadAll();
        Assert.AreEqual(10, partitionedService.PartitionsCount);
        Assert.AreEqual(100 * 100 * 100, results.Length);
        foreach (var result in results)
        {
            Assert.AreEqual(result.PartitionId, result.ThreadId);
            Assert.AreEqual(result.PartitionId, result.Id % 10);
        }
        
        var resultsInAPartition = partitionedService.ReadBulk(5, 0, 100);
        foreach (var result in resultsInAPartition)
        {
            Assert.AreEqual(5, result.PartitionId);
            Assert.AreEqual(5, result.Id % 10);
        }
        
        Console.WriteLine(partitionedService.OutputStatistics());
        Directory.Delete(testPath, true);
    }
}