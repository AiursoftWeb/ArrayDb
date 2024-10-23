using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

public class SampleData
{
    public int MyNumber1 { get; init; }
    public string MyString1 { get; init; } = string.Empty;
    public int MyNumber2 { get; init; }
    public bool MyBoolean1 { get; init; }
    public string MyString2 { get; init; } = string.Empty;
    public DateTime MyDateTime { get; set; } = DateTime.UtcNow;
}

[TestClass]
public class IntegrationTests
{
    [TestCleanup]
    public void CleanUp()
    {
        File.Delete("sampleData.bin");
        File.Delete("sampleDataStrings.bin");
    }
    
    [TestMethod]
    public void WriteAndReadTests()
    {
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        for (var i = 0; i < 1; i++)
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

        for (var i = 0; i < 1; i++)
        {
            var readSample = persistService.Read(i);
            Assert.AreEqual(i, readSample.MyNumber1);
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1);
            Assert.AreEqual(i * 10, readSample.MyNumber2);
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1);
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2);
        }
    }

    [TestMethod]
    public void PerformanceTestWrite()
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Write 100000 times
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
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 1000);
    }
    
    [TestMethod]
    public void PerformanceTestRead()
    {
        // Write 100000 times
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
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 1000);
    }
}