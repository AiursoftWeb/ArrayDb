using System.Diagnostics;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests.PerformanceTests;

[TestClass]
[DoNotParallelize]
public class PerformanceTests : ArrayDbTestBase
{
    [TestMethod]
    [Obsolete(message: "I understand that reading item one by one is slow, but this test need to cover the scenario.")]
    public void PerformanceTestWrite()
    {
        var stopWatch = new Stopwatch();
        // Write 10 0000 times in less than 20 seconds. On my machine: 4214ms -> 3707ms -> 2835ms
        stopWatch.Start();
        var persistService =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
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
        Console.WriteLine($"Write 100000 time: {stopWatch.ElapsedMilliseconds}ms");
        Console.WriteLine(persistService.OutputStatistics());
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 20 * 1000);
    }

    [TestMethod]
    public void PerformanceTestBulkWrite()
    {
        var persistService =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
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
        // Write 100 0000 times in less than 30 seconds. On my machine: 42148ms -> 37292ms -> 24595ms -> 15177ms -> 14934ms -> 14595ms -> 1060ms -> 680ms -> 643ms -> 530ms -> 352ms -> 308ms
        stopWatch.Start();
        persistService.Add(samplesArray);
        stopWatch.Stop();
        Console.WriteLine($"Write 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Assert.IsTrue(stopWatch.Elapsed.TotalSeconds < 30);
    }
    
    [TestMethod]
    public void PerformanceTestBulkRead()
    {
        var persistService =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var samples = new List<SampleData>();
        for (var i = 0; i < 1000000; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界！ {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            samples.Add(sample);
        }
        var samplesArray = samples.ToArray();
        persistService.Add(samplesArray);
        
        var persistService2 = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        // Read 100 0000 times in less than 20 seconds. On my machine 681ms -> 685ms.
        var result = persistService2.ReadBulk(0, 1000000);
        stopWatch.Stop();
        Console.WriteLine($"Read 1000000 times: {stopWatch.ElapsedMilliseconds}ms");
        Console.WriteLine(persistService2.OutputStatistics());
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 20 * 1000);
        
        for (var i = 0; i < 1000000; i++)
        {
            var readSample = result[i];
            Assert.AreEqual(i, readSample.MyNumber1);
            Assert.AreEqual($"Hello, World! 你好世界！ {i}", readSample.MyString1);
            Assert.AreEqual(i * 10, readSample.MyNumber2);
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1);
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2);
        }
    }
    
    [TestMethod]
    [Obsolete(message: "I understand that reading item one by one is slow, but this test need to cover the scenario.")]
    public void PerformanceTestRead()
    {
        // Read 100 0000 times in less than 20 seconds. On my machine: 2881ms
        var persistService =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
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
        persistService.Add(list.ToArray());
        
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
        Console.WriteLine(persistService.OutputStatistics());
        Assert.IsTrue(stopWatch.ElapsedMilliseconds < 20 * 1000);
    }
    
    [TestMethod]
    public void OutputStatistics()
    {
        var persistService =
            new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var samples = new List<SampleData>();
        for (var i = 0; i < 10; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界！ {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            samples.Add(sample);
        }
        var samplesArray = samples.ToArray();
        persistService.Add(samplesArray);
        persistService.ReadBulk(0, 10);
        
        var statistics = persistService.OutputStatistics();
        Console.WriteLine(statistics);
    }
}