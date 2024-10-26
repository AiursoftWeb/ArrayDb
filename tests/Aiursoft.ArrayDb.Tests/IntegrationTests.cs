using Aiursoft.ArrayDb.Engine.ObjectStorage;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class IntegrationTests : ArrayDbTestBase
{
    [TestMethod]
    [Obsolete(message: "I understand that writing item one by one is slow, but this test need to cover the scenario.")]
    public void WriteAndReadTests()
    {
        var persistService =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

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
    [Obsolete(message: "I understand that writing item one by one is slow, but this test need to cover the scenario.")]
    public void WriteAndReadEmptyString()
    {
        var persistService =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        var sample = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = string.Empty,
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null // All null strings will be converted to empty string.
        };
        persistService.Add(sample);
        var readSample = persistService.Read(0);
        Assert.AreEqual(1, readSample.MyNumber1);
        Assert.AreEqual(string.Empty, readSample.MyString1);
        Assert.AreEqual(2 * 10, readSample.MyNumber2);
        Assert.AreEqual(3 % 2 == 0, readSample.MyBoolean1);
        Assert.AreEqual(string.Empty, readSample.MyString2);
    }

    [TestMethod]
    [Obsolete(message: "I understand that writing item one by one is slow, but this test need to cover the scenario.")]
    public void RebootTest()
    {
        var persistService =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        var sample = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "我和我的祖国 Oh",
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null // All null strings will be converted to empty string.
        };
        var sample2 = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "My country and I 啊",
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null // All null strings will be converted to empty string.
        };
        persistService.Add(sample);
        persistService.Add(sample2);
        
        var persistService2 =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
        var length = persistService2.Count;
        Assert.AreEqual(2, length);
        var offSet = persistService2.StringRepository.FileEndOffset;
        Assert.AreEqual(49, offSet);
        
        var readSample = persistService2.Read(0);
        Assert.AreEqual(1, readSample.MyNumber1);
        Assert.AreEqual("我和我的祖国 Oh", readSample.MyString1);
        Assert.AreEqual(2 * 10, readSample.MyNumber2);
        Assert.AreEqual(3 % 2 == 0, readSample.MyBoolean1);
        Assert.AreEqual(string.Empty, readSample.MyString2);
        
        var readSample2 = persistService2.Read(1);
        Assert.AreEqual(1, readSample2.MyNumber1);
        Assert.AreEqual("My country and I 啊", readSample2.MyString1);
        Assert.AreEqual(2 * 10, readSample2.MyNumber2);
        Assert.AreEqual(3 % 2 == 0, readSample2.MyBoolean1);
        Assert.AreEqual(string.Empty, readSample2.MyString2);
    }

    [TestMethod]
    public void AddBulkAndReadBulk()
    {
        var persistService =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
        var samples = new List<SampleData>();
        for (var i = 0; i < 2; i++)
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
        
        var persistService2 =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        var readSamples = persistService2.ReadBulk(0, 2);
        for (var i = 0; i < 2; i++)
        {
            var readSample = readSamples[i];
            Assert.AreEqual(i, readSample.MyNumber1);
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1);
            Assert.AreEqual(i * 10, readSample.MyNumber2);
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1);
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2);
        }
    }

    [TestMethod]
    [Obsolete(message: "I understand that writing item one by one is slow, but this test need to cover the scenario.")]
    public void MixedBulkWriteAndWrite()
    {
        // Bulk write 2 samples
        var persistService =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
        var samples = new List<SampleData>();
        for (var i = 0; i < 2; i++)
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
        
        // Write 3 samples
        for (var i = 2; i < 5; i++)
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
        
        // Read all 5 samples
        var persistService2 =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
        var readSamples = persistService2.ReadBulk(0, 5);
        for (var i = 0; i < 5; i++)
        {
            var readSample = readSamples[i];
            Assert.AreEqual(i, readSample.MyNumber1);
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1);
            Assert.AreEqual(i * 10, readSample.MyNumber2);
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1);
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2);
        }
        Assert.AreEqual(5, persistService2.Count);
    }

    [TestMethod]
    [Obsolete(message: "I understand that writing item one by one is slow, but this test need to cover the scenario.")]
    public void ReadOutOfRangeTest()
    {
        var persistService =
            new ObjectRepository<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        // Write 2 samples
        var samples = new List<SampleData>();
        for (var i = 0; i < 2; i++)
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
        
        // Read item 2
        try
        {
            _ = persistService.Read(2);
            Assert.Fail("Should throw exception.");
        }
        catch (ArgumentOutOfRangeException e)
        {
           Assert.AreEqual("Specified argument was out of the range of valid values. (Parameter 'index')", e.Message); 
        }
        
        // Bulk read, starts from 1, read 2 items
        try
        {
            _ = persistService.ReadBulk(1, 2);
            Assert.Fail("Should throw exception.");
        }
        catch (ArgumentOutOfRangeException e)
        {
            Assert.AreEqual("Specified argument was out of the range of valid values. (Parameter 'indexFrom')", e.Message);
        }
        
        // However, normal read should not throw exception.
        var readSample = persistService.Read(1);
        Assert.AreEqual(1, readSample.MyNumber1);
    }
}