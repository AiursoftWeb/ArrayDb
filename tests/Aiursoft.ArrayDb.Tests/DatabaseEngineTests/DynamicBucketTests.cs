using System.Text;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class DynamicBucketTests : ArrayDbTestBase
{
    [TestMethod]
    public void TestWriteAndRead()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "MyNumber1", BucketItemPropertyType.Int32 },
                { "MyString1", BucketItemPropertyType.String },
                { "MyFloat", BucketItemPropertyType.Single },
                { "MyNumber2", BucketItemPropertyType.Int32 },
                { "MyBoolean1", BucketItemPropertyType.Boolean },
                { "MyString2", BucketItemPropertyType.String },
                { "MyFixedByteArray", BucketItemPropertyType.FixedSizeByteArray }
            },
            FixedByteArrayLengths = new Dictionary<string, int>
            {
                { "MyFixedByteArray", 30 }
            }
        };
        var persistService = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        for (var i = 0; i < 100; i++)
        {
            persistService.Add(new BucketItem
            {
                Properties = new Dictionary<string, BucketItemPropertyValue>
                {
                    {
                        "MyNumber1",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Int32,
                            Value = i
                        }
                    },
                    {
                        "MyString1",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.String,
                            Value = $"Hello, World! 你好世界 {i}"
                        }
                    },
                    {
                        "MyFloat",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Single,
                            Value = i * 0.1f
                        }
                    },
                    {
                        "MyNumber2",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Int32,
                            Value = i * 10
                        }
                    },
                    {
                        "MyBoolean1",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Boolean,
                            Value = i % 2 == 0
                        }
                    },
                    {
                        "MyString2",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.String,
                            Value = $"This is another longer string. {i}"
                        }
                    },
                    {
                        "MyFixedByteArray",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.FixedSizeByteArray,
                            Value = Encoding.UTF8.GetBytes($"FixedByteArray {i}")
                        }
                    }
                }
            });
        }

        Assert.AreEqual(100, persistService.Count, "The count of the bucket should match the expected value.");

        for (var i = 0; i < 100; i++)
        {
            var readSample = persistService.Read(i);
            Assert.AreEqual(i, readSample.Properties["MyNumber1"].Value,
                "The value of MyNumber1 should match the expected value.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.Properties["MyString1"].Value,
                "The value of MyString1 should match the expected value.");
            Assert.AreEqual(i * 0.1f, readSample.Properties["MyFloat"].Value,
                "The value of MyFloat should match the expected value.");
            Assert.AreEqual(i * 10, readSample.Properties["MyNumber2"].Value,
                "The value of MyNumber2 should match the expected value.");
            Assert.AreEqual(i % 2 == 0, readSample.Properties["MyBoolean1"].Value,
                "The value of MyBoolean1 should match the expected value.");
            Assert.AreEqual($"This is another longer string. {i}", readSample.Properties["MyString2"].Value,
                "The value of MyString2 should match the expected value.");
            Assert.AreEqual($"FixedByteArray {i}", Encoding.UTF8.GetString((readSample.Properties["MyFixedByteArray"].Value as byte[])!.TrimEndZeros()),
                "The value of MyFixedByteArray should match the expected value.");
        }

        var readSamples = persistService.ReadBulk(0, 100);
        for (var i = 0; i < 100; i++)
        {
            Assert.AreEqual(i, readSamples[i].Properties["MyNumber1"].Value,
                "The value of MyNumber1 should match the expected value.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSamples[i].Properties["MyString1"].Value,
                "The value of MyString1 should match the expected value.");
            Assert.AreEqual(i * 10, readSamples[i].Properties["MyNumber2"].Value,
                "The value of MyNumber2 should match the expected value.");
            Assert.AreEqual(i % 2 == 0, readSamples[i].Properties["MyBoolean1"].Value,
                "The value of MyBoolean1 should match the expected value.");
            Assert.AreEqual($"This is another longer string. {i}", readSamples[i].Properties["MyString2"].Value,
                "The value of MyString2 should match the expected value.");
            Assert.AreEqual($"FixedByteArray {i}", Encoding.UTF8.GetString((readSamples[i].Properties["MyFixedByteArray"].Value as byte[])!.TrimEndZeros()),
                "The value of MyFixedByteArray should match the expected value.");
        }
    }

    [TestMethod]
    public void TestItemSizeComputesCorrectlyForAllPropertyTypes()
    {
        // 计算预期大小: Int32(4) + Bool(1) + String(8+4) + DateTime(8) + Int64(8) + Single(4) + Double(8) + TimeSpan(8) + Guid(16) + ByteArray(50) = 119
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "IntProp", BucketItemPropertyType.Int32 },
                { "BoolProp", BucketItemPropertyType.Boolean },
                { "StringProp", BucketItemPropertyType.String },
                { "DateProp", BucketItemPropertyType.DateTime },
                { "LongProp", BucketItemPropertyType.Int64 },
                { "FloatProp", BucketItemPropertyType.Single },
                { "DoubleProp", BucketItemPropertyType.Double },
                { "TimeSpanProp", BucketItemPropertyType.TimeSpan },
                { "GuidProp", BucketItemPropertyType.Guid },
                { "ByteArrayProp", BucketItemPropertyType.FixedSizeByteArray }
            },
            FixedByteArrayLengths = new Dictionary<string, int>
            {
                { "ByteArrayProp", 50 }
            }
        };

        var expectedItemSize = sizeof(int) + sizeof(bool) + sizeof(long) + sizeof(int) + sizeof(long) + sizeof(long) + sizeof(float) + sizeof(double) + sizeof(long) + 16 + 50;
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var actualItemSize = bucket.GetItemSize();
        Assert.AreEqual(expectedItemSize, actualItemSize, "GetItemSize() should return the correct item size for all property types.");

        bucket.Add(new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "IntProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = 42 } },
                { "BoolProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Boolean, Value = true } },
                { "StringProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = "hello" } },
                { "DateProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.DateTime, Value = new DateTime(2024, 1, 1) } },
                { "LongProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int64, Value = 999L } },
                { "FloatProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Single, Value = 3.14f } },
                { "DoubleProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Double, Value = 2.718 } },
                { "TimeSpanProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.TimeSpan, Value = TimeSpan.FromMinutes(5) } },
                { "GuidProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Guid, Value = Guid.Empty } },
                { "ByteArrayProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.FixedSizeByteArray, Value = new byte[50] } }
            }
        });

        var readBack = bucket.Read(0);
        Assert.AreEqual(42, readBack.Properties["IntProp"].Value, "Read-back should work correctly with cached item size.");
        Assert.AreEqual("hello", readBack.Properties["StringProp"].Value, "Read-back string should work correctly with cached item size.");
        Assert.AreEqual(999L, readBack.Properties["LongProp"].Value, "Read-back Int64 should work correctly with cached item size.");

        var stats = bucket.OutputStatistics();
        var expectedBytes = expectedItemSize * bucket.SpaceProvisionedItemsCount;
        Assert.IsTrue(stats.Contains($"Consumed actual storage space: {expectedBytes} bytes"),
            "OutputStatistics should report correct consumed space using cached item size.");
    }
}
