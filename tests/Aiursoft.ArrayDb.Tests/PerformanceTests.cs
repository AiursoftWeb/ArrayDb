using System.Diagnostics;
using Aiursoft.ArrayDb.Extensions;
using Aiursoft.ArrayDb.ObjectStorage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class PerformanceTests : ArrayDbTestBase
{
    [TestMethod]
    [Obsolete(message: "I understand that reading item one by one is slow, but this test need to cover the scenario.")]
    public void PerformanceTestWrite()
    {
        var stopWatch = new Stopwatch();
        // Write 100 0000 times in less than 100 seconds. On my machine: 42,148ms -> 37,072ms
        stopWatch.Start();
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
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
            persistService.Add(sample);
        }
        stopWatch.Stop();
        Console.WriteLine($"Write 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 100 * 1000);
    }

    [TestMethod]
    public void TenTimesBulkWriteAverage()
    {
        var time = ToolkitExtensions.RunWithTimedBench("Bench multiple bulk write", () =>
        {
            for (var i = 0; i < 10; i++)
            {
                Init();
                PerformanceTestBulkWrite();
            }
        });
        Assert.IsTrue(time.TotalSeconds < 40); // On my machine, it's usually 6s.
    }
    
    [TestMethod]
    public void PerformanceTestBulkWrite()
    {
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        var samples = new List<SampleData>();
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
            samples.Add(sample);
        }
        var samplesArray = samples.ToArray();
        
        var stopWatch = new Stopwatch();
        // Write 100 0000 times in less than 5 seconds. On my machine: 42148ms -> 37292ms -> 24595ms -> 15177ms -> 14934ms -> 14595ms -> 1060ms -> 680ms -> 643ms
        stopWatch.Start();
        persistService.AddBulk(samplesArray);
        stopWatch.Stop();
        Console.WriteLine($"Write 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 5);
    }
    
    [TestMethod]
    public void PerformanceTestBulkRead()
    {
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        var samples = new List<SampleData>();
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
            samples.Add(sample);
        }
        var samplesArray = samples.ToArray();
        persistService.AddBulk(samplesArray);
        
        var persistService2 = new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Read 100 0000 times in less than 10 seconds. On my machine 681ms -> 685ms.
        var result = persistService2.ReadBulk(0, 1000000);
        stopWatch.Stop();
        Console.WriteLine($"Read 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 10 * 1000);
        
        for (var i = 0; i < 1000000; i++)
        {
            var readSample = result[i];
            Assert.AreEqual(i, readSample.MyNumber1);
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1);
            Assert.AreEqual(i * 10, readSample.MyNumber2);
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1);
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2);
        }
    }
    
    [TestMethod]
    [Obsolete(message: "I understand that reading item one by one is slow, but this test need to cover the scenario.")]
    public void PerformanceTestRead()
    {
        // Read 100 0000 times in less than 10 seconds. On my machine: 760ms -> 912ms
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        var list = new List<SampleData>();
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
            list.Add(sample);
        }
        persistService.AddBulk(list.ToArray());
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Read 100 0000 times
        for (var i = 0; i < 1000000; i++)
        {
            var readSample = persistService.Read(i);
            Assert.AreEqual(i, readSample.MyNumber1);
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1);
            Assert.AreEqual(i * 10, readSample.MyNumber2);
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1);
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2);
        }
        stopWatch.Stop();

        Console.WriteLine($"Read 100000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 10 * 1000);
    }
}