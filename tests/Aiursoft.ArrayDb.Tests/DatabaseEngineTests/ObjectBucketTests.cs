using System.Reflection;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class ObjectBucketTests : ArrayDbTestBase
{
    [TestMethod]
    public void AddAndRead_ShouldWorkCorrectly_WithCachedProperties()
    {
        var bucket = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        var sample = new SampleData
        {
            MyNumber1 = 42,
            MyString1 = "Hello, cached!",
            MyNumber2 = 84,
            MyBoolean1 = true,
            MyString2 = "Another string"
        };

        bucket.Add(sample);
        var read = bucket.Read(0);

        Assert.AreEqual(42, read.MyNumber1);
        Assert.AreEqual("Hello, cached!", read.MyString1);
        Assert.AreEqual(84, read.MyNumber2);
        Assert.AreEqual(true, read.MyBoolean1);
        Assert.AreEqual("Another string", read.MyString2);
    }

    [TestMethod]
    public void BulkAddAndRead_ShouldWorkCorrectly_WithCachedProperties()
    {
        var bucket = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        var samples = Enumerable.Range(0, 100).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"String-{i}",
            MyNumber2 = i * 10,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"Longer-{i}"
        }).ToArray();

        bucket.Add(samples);
        var read = bucket.ReadBulk(0, 100);

        for (var i = 0; i < 100; i++)
        {
            Assert.AreEqual(i, read[i].MyNumber1);
            Assert.AreEqual($"String-{i}", read[i].MyString1);
            Assert.AreEqual(i * 10, read[i].MyNumber2);
            Assert.AreEqual(i % 2 == 0, read[i].MyBoolean1);
            Assert.AreEqual($"Longer-{i}", read[i].MyString2);
        }
    }

    [TestMethod]
    public void DifferentGenericTypes_ShouldHaveSeparateCachedProperties()
    {
        // Get the static _persistedProperties field via reflection for two different types
        var sampleDataField = typeof(ObjectBucket<SampleData>)
            .GetField("_persistedProperties", BindingFlags.NonPublic | BindingFlags.Static);
        var bytesDataField = typeof(ObjectBucket<BytesData>)
            .GetField("_persistedProperties", BindingFlags.NonPublic | BindingFlags.Static);

        Assert.IsNotNull(sampleDataField);
        Assert.IsNotNull(bytesDataField);

        var sampleDataProps = (PropertyInfo[])sampleDataField.GetValue(null)!;
        var bytesDataProps = (PropertyInfo[])bytesDataField.GetValue(null)!;

        // SampleData has 5 properties (sorted by name): CreationTime, MyBoolean1, MyNumber1, MyNumber2, MyString1, MyString2
        // Actually: CreationTime, MyBoolean1, MyNumber1, MyNumber2, MyString1, MyString2 = 6
        // Let's verify by name
        var sampleDataPropNames = sampleDataProps.Select(p => p.Name).OrderBy(n => n).ToArray();
        var bytesDataPropNames = bytesDataProps.Select(p => p.Name).OrderBy(n => n).ToArray();

        // Verify they are different property sets
        CollectionAssert.AreNotEqual(sampleDataPropNames, bytesDataPropNames);

        // Verify SampleData properties
        CollectionAssert.Contains(sampleDataPropNames, "CreationTime");
        CollectionAssert.Contains(sampleDataPropNames, "MyNumber1");
        CollectionAssert.Contains(sampleDataPropNames, "MyString1");

        // Verify BytesData properties
        CollectionAssert.Contains(bytesDataPropNames, "AdeId");
        CollectionAssert.Contains(bytesDataPropNames, "BytesText");
        CollectionAssert.Contains(bytesDataPropNames, "ZdexId");
    }

    [TestMethod]
    public void AddAndRead_BytesData_ShouldWorkCorrectly()
    {
        var bucket = new ObjectBucket<BytesData>(TestFilePath, TestFilePathStrings);

        var data = new BytesData
        {
            AdeId = 1,
            BytesText = new byte[] { 0x01, 0x02, 0x03 },
            ZdexId = 2
        };

        bucket.Add(data);
        var read = bucket.Read(0);

        Assert.AreEqual(1, read.AdeId);
        Assert.AreEqual(2, read.ZdexId);
        Assert.AreEqual(50, read.BytesText.Length, "Fixed-size byte array is padded to declared length");
        Assert.AreEqual(0x01, read.BytesText[0]);
        Assert.AreEqual(0x02, read.BytesText[1]);
        Assert.AreEqual(0x03, read.BytesText[2]);
        // Remaining bytes should be zero-padded
        for (var i = 3; i < 50; i++)
            Assert.AreEqual((byte)0, read.BytesText[i]);
    }
}
