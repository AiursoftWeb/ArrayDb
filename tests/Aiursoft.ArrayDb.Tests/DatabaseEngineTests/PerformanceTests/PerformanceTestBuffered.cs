using System.Diagnostics;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Aiursoft.ArrayDb.WriteBuffer;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests.PerformanceTests;

[TestClass]
[DoNotParallelize]
public class PerformanceTestBuffered : ArrayDbTestBase
{
    [TestMethod]
    public async Task PerformanceTestParallelBufferedWrite()
    {
        var bucket =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(bucket);
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        Parallel.For(0, 100 * 100 * 100, i =>
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            buffer.Add(sample);
        });
        stopWatch.Stop();
        Console.WriteLine(buffer.OutputStatistics());
        Console.WriteLine($"Write 100 * 100 * 100 times: {stopWatch.ElapsedMilliseconds}ms");
        // Read 100 0000 times in less than 10 seconds. On my machine 345ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 10);
        Assert.IsTrue(buffer.IsHot);

        stopWatch.Reset();
        stopWatch.Start();
        await buffer.SyncAsync();
        stopWatch.Stop();

        Console.WriteLine($"Sync buffer: {stopWatch.ElapsedMilliseconds}ms");
        // Sync 100 0000 times in less than 30 seconds. On my machine 119ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 30);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.IsTrue(buffer.IsCold);
        Assert.AreEqual(100 * 100 * 100, bucket.SpaceProvisionedItemsCount);
        Console.WriteLine(buffer.OutputStatistics());

        // It should do nothing.
        await buffer.SyncAsync();
    }
    
    [TestMethod]
    public async Task PerformanceTestSequentialBufferedWrite()
    {
        var bucket =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(bucket);
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        for (var i = 0; i < 100 * 100 * 100; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            buffer.Add(sample);
        }
        stopWatch.Stop();
        Console.WriteLine($"Write 100 * 100 * 100 times: {stopWatch.ElapsedMilliseconds}ms");
        // Read 100 0000 times in less than 10 seconds. On my machine 597ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 50);
        Assert.IsTrue(buffer.IsHot);
        
        Console.WriteLine(buffer.OutputStatistics());
        
        stopWatch.Reset();
        stopWatch.Start();
        await buffer.SyncAsync();
        stopWatch.Stop();

        Console.WriteLine($"Sync buffer: {stopWatch.ElapsedMilliseconds}ms");
        // Sync 1000 0000 times in less than 10 seconds. On my machine 119ms.
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 100 * 100 * 100);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.IsTrue(buffer.IsCold);
        Assert.AreEqual(1000000, bucket.SpaceProvisionedItemsCount);
        Console.WriteLine(buffer.OutputStatistics());
    }
}