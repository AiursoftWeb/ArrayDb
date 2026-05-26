using System.Collections.Concurrent;
using System.Text;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.WriteBuffer.Dynamic;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class BufferedDynamicObjectBucketReadSnapshotTests : ArrayDbTestBase
{
    private BucketItemTypeDefinition GetSampleTypeDefinition()
    {
        return new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "MyNumber1", BucketItemPropertyType.Int32 },
                { "MyString1", BucketItemPropertyType.String },
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
    }

    private BucketItem CreateSampleItem(int i)
    {
        return new BucketItem
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
                        Value = $"Hello, World! {i}"
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
        };
    }

    [TestMethod]
    public void ReadSingleFromBufferOnlyReturnsCorrectItem()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        var items = Enumerable.Range(0, 100).Select(CreateSampleItem).ToArray();
        bufferedBucket.Add(items);

        // Read each item from the buffer and verify correctness
        for (var i = 0; i < 100; i++)
        {
            var read = bufferedBucket.Read(i);
            Assert.AreEqual(i, read.Properties["MyNumber1"].Value,
                $"Item at index {i} should have correct MyNumber1.");
            Assert.AreEqual($"Hello, World! {i}", read.Properties["MyString1"].Value,
                $"Item at index {i} should have correct MyString1.");
        }
    }

    [TestMethod]
    public void ReadSingleSpanningInnerAndBuffer()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);

        // Pre-populate inner bucket with 50 items
        var innerItems = Enumerable.Range(0, 50).Select(i => new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "MyNumber1", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = i } },
                { "MyString1", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"Inner {i}" } },
                { "MyNumber2", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = i * 2 } },
                { "MyBoolean1", new BucketItemPropertyValue { Type = BucketItemPropertyType.Boolean, Value = true } },
                { "MyString2", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"InnerString {i}" } },
                { "MyFixedByteArray", new BucketItemPropertyValue { Type = BucketItemPropertyType.FixedSizeByteArray, Value = Encoding.UTF8.GetBytes($"Fixed {i}") } }
            }
        }).ToArray();
        dynamicBucket.Add(innerItems);

        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add 50 items to buffer
        var bufferItems = Enumerable.Range(50, 50).Select(i => new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "MyNumber1", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = i } },
                { "MyString1", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"Buffer {i}" } },
                { "MyNumber2", new BucketItemPropertyValue { Type = BucketItemPropertyType.Int32, Value = i * 2 } },
                { "MyBoolean1", new BucketItemPropertyValue { Type = BucketItemPropertyType.Boolean, Value = false } },
                { "MyString2", new BucketItemPropertyValue { Type = BucketItemPropertyType.String, Value = $"BufferString {i}" } },
                { "MyFixedByteArray", new BucketItemPropertyValue { Type = BucketItemPropertyType.FixedSizeByteArray, Value = Encoding.UTF8.GetBytes($"Buf {i}") } }
            }
        }).ToArray();
        bufferedBucket.Add(bufferItems);

        // Read from inner bucket
        var readInner = bufferedBucket.Read(25);
        Assert.AreEqual(25, readInner.Properties["MyNumber1"].Value);
        Assert.AreEqual("Inner 25", readInner.Properties["MyString1"].Value);

        // Read from buffer
        var readBuffer = bufferedBucket.Read(75);
        Assert.AreEqual(75, readBuffer.Properties["MyNumber1"].Value);
        Assert.AreEqual("Buffer 75", readBuffer.Properties["MyString1"].Value);

        // Read boundary (first buffer item)
        var readBoundary = bufferedBucket.Read(50);
        Assert.AreEqual(50, readBoundary.Properties["MyNumber1"].Value);
        Assert.AreEqual("Buffer 50", readBoundary.Properties["MyString1"].Value);
    }

    [TestMethod]
    public void ReadThrowsForOutOfRangeIndex()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        bufferedBucket.Add(CreateSampleItem(1));

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => bufferedBucket.Read(-1),
            "Reading negative index should throw.");
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => bufferedBucket.Read(100),
            "Reading index beyond count should throw.");
    }

    [TestMethod]
    public async Task ReadSnapshotHandlesLargeBuffer()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        var items = Enumerable.Range(0, 10000).Select(CreateSampleItem).ToArray();
        bufferedBucket.Add(items);

        // Read the first, middle, and last items to verify correctness with large buffer
        var first = bufferedBucket.Read(0);
        Assert.AreEqual(0, first.Properties["MyNumber1"].Value);
        Assert.AreEqual("Hello, World! 0", first.Properties["MyString1"].Value);

        var middle = bufferedBucket.Read(5000);
        Assert.AreEqual(5000, middle.Properties["MyNumber1"].Value);
        Assert.AreEqual("Hello, World! 5000", middle.Properties["MyString1"].Value);

        var last = bufferedBucket.Read(9999);
        Assert.AreEqual(9999, last.Properties["MyNumber1"].Value);
        Assert.AreEqual("Hello, World! 9999", last.Properties["MyString1"].Value);

        // Verify data is still correct after sync
        await bufferedBucket.SyncAsync();

        var firstAfterSync = bufferedBucket.Read(0);
        Assert.AreEqual(0, firstAfterSync.Properties["MyNumber1"].Value);
    }

    [TestMethod]
    public void ReadSnapshotConsistencyUnderConcurrentAdds()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Pre-add some items
        var initialItems = Enumerable.Range(0, 50).Select(CreateSampleItem).ToArray();
        bufferedBucket.Add(initialItems);

        // Concurrently read from multiple threads
        var readTasks = new List<Task>();
        var errors = new ConcurrentBag<Exception>();

        for (var t = 0; t < 10; t++)
        {
            readTasks.Add(Task.Run(() =>
            {
                try
                {
                    for (var i = 0; i < 50; i++)
                    {
                        var item = bufferedBucket.Read(i);
                        Assert.AreEqual(i, item.Properties["MyNumber1"].Value,
                            $"Item at index {i} should have correct MyNumber1 under concurrent reads.");
                    }
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }));
        }

        Task.WaitAll(readTasks.ToArray());

        Assert.AreEqual(0, errors.Count,
            $"Concurrent reads should not produce errors, but got {errors.Count}: {string.Join(", ", errors.Select(e => e.Message))}");
        Assert.AreEqual(50, bufferedBucket.Count);
    }
}
