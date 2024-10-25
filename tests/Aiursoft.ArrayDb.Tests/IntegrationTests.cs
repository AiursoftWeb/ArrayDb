using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class IntegrationTests : ArrayDbTestBase
{
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
    public void WriteAndReadEmptyString()
    {
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        var sample = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = string.Empty,
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null
        };
        persistService.Add(sample);
        var readSample = persistService.Read(0);
        Assert.AreEqual(1, readSample.MyNumber1);
        Assert.AreEqual(string.Empty, readSample.MyString1);
        Assert.AreEqual(2 * 10, readSample.MyNumber2);
        Assert.AreEqual(3 % 2 == 0, readSample.MyBoolean1);
        Assert.AreEqual(null, readSample.MyString2);
    }

    [TestMethod]
    public void RebootTest()
    {
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

        var sample = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "我和我的祖国",
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null
        };
        var sample2 = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "我和我的祖国啊",
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null
        };
        persistService.Add(sample);
        persistService.Add(sample2);
        
        var persistService2 =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
        var length = persistService2.Length;
        Assert.AreEqual(2, length);
        var offSet = persistService2.StringRepository.FileEndOffset;
        Assert.AreEqual(47, offSet);
        
        var readSample = persistService2.Read(0);
        Assert.AreEqual(1, readSample.MyNumber1);
        Assert.AreEqual("我和我的祖国", readSample.MyString1);
        Assert.AreEqual(2 * 10, readSample.MyNumber2);
        Assert.AreEqual(3 % 2 == 0, readSample.MyBoolean1);
        Assert.AreEqual(null, readSample.MyString2);
        
        var readSample2 = persistService2.Read(1);
        Assert.AreEqual(1, readSample2.MyNumber1);
        Assert.AreEqual("我和我的祖国啊", readSample2.MyString1);
        Assert.AreEqual(2 * 10, readSample2.MyNumber2);
        Assert.AreEqual(3 % 2 == 0, readSample2.MyBoolean1);
        Assert.AreEqual(null, readSample2.MyString2);
    }

    [TestMethod]
    public void AddBulkAndReadBulk()
    {
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
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
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);

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
    public void MixedBulkWriteAndWrite()
    {
        // Bulk write 2 samples
        var persistService =
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
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
            new ObjectPersistOnDiskService<SampleData>("sampleData.bin", "sampleDataStrings.bin", 0x10000);
        
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
        Assert.AreEqual(5, persistService2.Length);
    }
}