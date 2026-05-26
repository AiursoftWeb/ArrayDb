using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class DynamicBucketParallelThresholdTests : ArrayDbTestBase
{
    private static BucketItemTypeDefinition CreateSimpleTypeDefine()
    {
        return new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "MyNumber1", BucketItemPropertyType.Int32 },
                { "MyString1", BucketItemPropertyType.String },
                { "MyFloat", BucketItemPropertyType.Single },
                { "MyNumber2", BucketItemPropertyType.Int32 },
                { "MyBoolean1", BucketItemPropertyType.Boolean },
                { "MyString2", BucketItemPropertyType.String }
            }
        };
    }

    private static BucketItem CreateItem(int index)
    {
        return new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "MyNumber1", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = index } },
                { "MyString1", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"Hello {index}" } },
                { "MyFloat", new BucketItemPropertyValue { Type = BucketItemPropertyType.Single, Value = index * 0.1f } },
                { "MyNumber2", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = index * 10 } },
                { "MyBoolean1", new BucketItemPropertyValue { Type = BucketItemPropertyType.Boolean, Value = index % 2 == 0 } },
                { "MyString2", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"World {index}" } }
            }
        };
    }

    private static void AssertItem(BucketItem item, int expectedIndex)
    {
        Assert.AreEqual(expectedIndex, item.Properties["MyNumber1"].Value);
        Assert.AreEqual($"Hello {expectedIndex}", item.Properties["MyString1"].Value);
        Assert.AreEqual(expectedIndex * 0.1f, item.Properties["MyFloat"].Value);
        Assert.AreEqual(expectedIndex * 10, item.Properties["MyNumber2"].Value);
        Assert.AreEqual(expectedIndex % 2 == 0, item.Properties["MyBoolean1"].Value);
        Assert.AreEqual($"World {expectedIndex}", item.Properties["MyString2"].Value);
    }

    [TestMethod]
    public void AddSingleItem_UsesSequentialPath()
    {
        var typeDefine = CreateSimpleTypeDefine();
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        // Single item is well below the ParallelSerializeThreshold (8), so it uses the sequential path.
        bucket.Add(CreateItem(42));
        Assert.AreEqual(1, bucket.Count);

        var readBack = bucket.Read(0);
        AssertItem(readBack, 42);
    }

    [TestMethod]
    public void AddSmallBatch_UsesSequentialPath()
    {
        var typeDefine = CreateSimpleTypeDefine();
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var items = new BucketItem[3];
        for (var i = 0; i < items.Length; i++)
            items[i] = CreateItem(i);

        // 3 items is below ParallelSerializeThreshold (8), uses sequential path.
        bucket.Add(items);
        Assert.AreEqual(3, bucket.Count);

        for (var i = 0; i < 3; i++)
        {
            var readBack = bucket.Read(i);
            AssertItem(readBack, i);
        }
    }

    [TestMethod]
    public void AddLargeBatch_UsesParallelPath()
    {
        var typeDefine = CreateSimpleTypeDefine();
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var items = new BucketItem[16];
        for (var i = 0; i < items.Length; i++)
            items[i] = CreateItem(i);

        // 16 items is above ParallelSerializeThreshold (8), uses parallel path.
        bucket.Add(items);
        Assert.AreEqual(16, bucket.Count);

        for (var i = 0; i < 16; i++)
        {
            var readBack = bucket.Read(i);
            AssertItem(readBack, i);
        }
    }

    [TestMethod]
    public void ReadBulkSmallBatch_UsesSequentialPath()
    {
        var typeDefine = CreateSimpleTypeDefine();
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var items = new BucketItem[16];
        for (var i = 0; i < items.Length; i++)
            items[i] = CreateItem(i);
        bucket.Add(items);

        // ReadBulk with 3 items (below threshold) uses sequential deserialization path.
        var results = bucket.ReadBulk(0, 3);
        Assert.AreEqual(3, results.Length);
        for (var i = 0; i < 3; i++)
            AssertItem(results[i], i);
    }

    [TestMethod]
    public void ReadBulkLargeBatch_UsesParallelPath()
    {
        var typeDefine = CreateSimpleTypeDefine();
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var items = new BucketItem[16];
        for (var i = 0; i < items.Length; i++)
            items[i] = CreateItem(i);
        bucket.Add(items);

        // ReadBulk with 16 items (above threshold) uses parallel deserialization path.
        var results = bucket.ReadBulk(0, 16);
        Assert.AreEqual(16, results.Length);
        for (var i = 0; i < 16; i++)
            AssertItem(results[i], i);
    }

    [TestMethod]
    public void SequentialAndParallelPaths_ProduceIdenticalResults()
    {
        var typeDefine = CreateSimpleTypeDefine();
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        // Write a batch above threshold (parallel path).
        var parallelItems = new BucketItem[10];
        for (var i = 0; i < parallelItems.Length; i++)
            parallelItems[i] = CreateItem(i);
        bucket.Add(parallelItems);

        // Writer single items (sequential path).
        for (var i = 10; i < 15; i++)
            bucket.Add(CreateItem(i));

        Assert.AreEqual(15, bucket.Count);

        // ReadBulk below threshold (sequential path).
        var sequentialRead = bucket.ReadBulk(0, 3);
        Assert.AreEqual(3, sequentialRead.Length);
        for (var i = 0; i < 3; i++)
            AssertItem(sequentialRead[i], i);

        // ReadBulk above threshold (parallel path).
        var parallelRead = bucket.ReadBulk(0, 15);
        Assert.AreEqual(15, parallelRead.Length);
        for (var i = 0; i < 15; i++)
            AssertItem(parallelRead[i], i);
    }
}
