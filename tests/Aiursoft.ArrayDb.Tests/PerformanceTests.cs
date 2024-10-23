using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class PerformanceTests : ArrayDbTestBase
{
    
    [TestMethod]
    public void PerformanceTestWrite()
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Write 100000 times in less than 8 seconds. Ideally, it should be around 2.3 seconds.
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        for (var i = 0; i < 100000; i++)
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
        Console.WriteLine($"Write 100000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 8000);
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
        // Write 100 0000 times in less than 30 seconds. Ideally, it should be around 15 seconds.
        stopWatch.Start();
        persistService.AddBulk(samplesArray);
        stopWatch.Stop();
        Console.WriteLine($"Write 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 30000);
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
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Read 1000000 times
        var result = persistService.ReadBulk(0, 1000000);
        stopWatch.Stop();
        Console.WriteLine($"Read 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        
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
    public void PerformanceTestRead()
    {
        // Write 100000 times should in less than 3 second. Ideally, it should be around 0.8 seconds.
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        for (var i = 0; i < 100000; i++)
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
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Read 100000 times
        for (var i = 0; i < 100000; i++)
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
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 3000);
    }
}