using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class DynamicBucketStringParallelLoadTests : ArrayDbTestBase
{
    [TestMethod]
    public void ReadBulk_ManyStringProperties_AllLoadedCorrectly()
    {
        // Define a type with 5 string properties to ensure parallel path triggers.
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "Str0", BucketItemPropertyType.String },
                { "Str1", BucketItemPropertyType.String },
                { "Str2", BucketItemPropertyType.String },
                { "Str3", BucketItemPropertyType.String },
                { "Str4", BucketItemPropertyType.String },
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var items = new BucketItem[20];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new BucketItem
            {
                Properties = new Dictionary<string, BucketItemPropertyValue>
                {
                    { "Str0", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"s0-item-{i}" } },
                    { "Str1", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"s1-item-{i}-extended" } },
                    { "Str2", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"s2-item-{i}" } },
                    { "Str3", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"s3-item-{i}" } },
                    { "Str4", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"s4-item-{i}" } },
                }
            };
        }

        bucket.Add(items);

        // ReadBulk triggers parallel deserialization and parallel string loading inside each record.
        var results = bucket.ReadBulk(0, items.Length);
        Assert.AreEqual(items.Length, results.Length);

        for (var i = 0; i < items.Length; i++)
        {
            Assert.AreEqual($"s0-item-{i}", results[i].Properties["Str0"].Value);
            Assert.AreEqual($"s1-item-{i}-extended", results[i].Properties["Str1"].Value);
            Assert.AreEqual($"s2-item-{i}", results[i].Properties["Str2"].Value);
            Assert.AreEqual($"s3-item-{i}", results[i].Properties["Str3"].Value);
            Assert.AreEqual($"s4-item-{i}", results[i].Properties["Str4"].Value);
        }
    }

    [TestMethod]
    public void SingleRead_OneStringProperty_LoadsCorrectly()
    {
        // Single string property: sequential path branch.
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "OnlyString", BucketItemPropertyType.String },
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        bucket.Add(new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "OnlyString", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = "hello-solo" } },
            }
        });

        var result = bucket.Read(0);
        Assert.AreEqual("hello-solo", result.Properties["OnlyString"].Value);
    }

    [TestMethod]
    public void ReadBulk_NoStringProperties_ReturnsCorrectNonStringValues()
    {
        // No string properties: string loading loop is a no-op.
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "IntVal", BucketItemPropertyType.Int32 },
                { "BoolVal", BucketItemPropertyType.Boolean },
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        bucket.Add(new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "IntVal", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = 42 } },
                { "BoolVal", new BucketItemPropertyValue { Type = BucketItemPropertyType.Boolean, Value = true } },
            }
        });

        var results = bucket.ReadBulk(0, 1);
        Assert.AreEqual(42, results[0].Properties["IntVal"].Value);
        Assert.AreEqual(true, results[0].Properties["BoolVal"].Value);
    }

    [TestMethod]
    public void ReadBulk_ConcurrentReads_NoStringMixUp()
    {
        // Read same records repeatedly from multiple threads; verify no cross-record string contamination.
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "A", BucketItemPropertyType.String },
                { "B", BucketItemPropertyType.String },
                { "C", BucketItemPropertyType.String },
                { "D", BucketItemPropertyType.String },
                { "IntId", BucketItemPropertyType.Int32 },
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var items = new BucketItem[30];
        for (var i = 0; i < items.Length; i++)
        {
            items[i] = new BucketItem
            {
                Properties = new Dictionary<string, BucketItemPropertyValue>
                {
                    { "A", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = new string('a', i + 10) } },
                    { "B", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = new string('b', i + 10) } },
                    { "C", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = new string('c', i + 10) } },
                    { "D", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = new string('d', i + 10) } },
                    { "IntId", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = i } },
                }
            };
        }

        bucket.Add(items);

        var mismatchCount = 0;
        var options = new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount };
        Parallel.For(0, 200, options, _ =>
        {
            var results = bucket.ReadBulk(0, items.Length);
            for (var i = 0; i < items.Length; i++)
            {
                var id = (int)results[i].Properties["IntId"].Value!;
                var expectedA = new string('a', id + 10);
                var expectedB = new string('b', id + 10);
                var expectedC = new string('c', id + 10);
                var expectedD = new string('d', id + 10);

                if (!expectedA.Equals(results[i].Properties["A"].Value) ||
                    !expectedB.Equals(results[i].Properties["B"].Value) ||
                    !expectedC.Equals(results[i].Properties["C"].Value) ||
                    !expectedD.Equals(results[i].Properties["D"].Value))
                {
                    Interlocked.Increment(ref mismatchCount);
                }
            }
        });

        Assert.AreEqual(0, mismatchCount, "No cross-record string contamination under concurrent reads.");
    }
}
