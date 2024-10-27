using System.Diagnostics;
using Aiursoft.ArrayDb.Engine;
using Aiursoft.ArrayDb.Tests.Models;
using Aiursoft.ArrayDb.WriteBuffer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class PerformanceTestBuffered : ArrayDbTestBase
{
    [TestMethod]
    public async Task PerformanceTestParallelBufferedWrite()
    {
        var bucket =
            new ObjectBuckets<SampleData>("sampleData.bin", "sampleDataStrings.bin");
        var buffer = new BufferedObjectBuckets<SampleData>(bucket);
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        Parallel.For(0, 10000000, i =>
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            buffer.AddBuffered(sample);
        });
        stopWatch.Stop();
        Console.WriteLine(buffer.OutputStatistics());
        Console.WriteLine($"Write 10000000 times: {stopWatch.ElapsedMilliseconds}ms");
        // Read 1000 0000 times in less than 50 seconds. On my machine 5975ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 50);
        
        Assert.IsTrue(buffer.IsHot);
        Assert.IsTrue(buffer.BufferedItemsCount > 1);
        Assert.IsTrue(bucket.Count < 10000000);
        
        stopWatch.Reset();
        stopWatch.Start();
        await buffer.SyncAsync();
        stopWatch.Stop();

        Console.WriteLine($"Sync buffer: {stopWatch.ElapsedMilliseconds}ms");
        // Sync 1000 0000 times in less than 50 seconds. On my machine 1192ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 50);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.AreEqual(10000000, bucket.Count);
        Console.WriteLine(buffer.OutputStatistics());
    }
    
    [TestMethod]
    public async Task PerformanceTestSequentialBufferedWrite()
    {
        var bucket =
            new ObjectBuckets<SampleData>("sampleData.bin", "sampleDataStrings.bin");
        var buffer = new BufferedObjectBuckets<SampleData>(bucket);
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        for (var i = 0; i < 10000000; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            buffer.AddBuffered(sample);
        }
        stopWatch.Stop();
        Console.WriteLine(buffer.OutputStatistics());
        Console.WriteLine($"Write 10000000 times: {stopWatch.ElapsedMilliseconds}ms");
        // Read 1000 0000 times in less than 50 seconds. On my machine 5975ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 50);
        
        Assert.IsTrue(buffer.IsHot);
        Assert.IsTrue(buffer.BufferedItemsCount > 1);
        Assert.IsTrue(bucket.Count < 10000000);
        
        stopWatch.Reset();
        stopWatch.Start();
        await buffer.SyncAsync();
        stopWatch.Stop();

        Console.WriteLine($"Sync buffer: {stopWatch.ElapsedMilliseconds}ms");
        // Sync 1000 0000 times in less than 50 seconds. On my machine 1192ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 50);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.AreEqual(10000000, bucket.Count);
        Console.WriteLine(buffer.OutputStatistics());
    }
}