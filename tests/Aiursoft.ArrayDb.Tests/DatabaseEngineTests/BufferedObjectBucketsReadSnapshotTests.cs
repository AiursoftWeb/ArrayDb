using System.Collections.Concurrent;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Aiursoft.ArrayDb.WriteBuffer;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class BufferedObjectBucketsReadSnapshotTests : ArrayDbTestBase
{
    [TestMethod]
    public void ReadSingleFromBufferOnlyReturnsCorrectItem()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = Enumerable.Range(0, 100).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        buffer.Add(items);

        // Read each item from the buffer (before any persistence) and verify correctness
        for (var i = 0; i < 100; i++)
        {
            var read = buffer.Read(i);
            Assert.AreEqual(i, read.MyNumber1, $"Item at index {i} should have correct MyNumber1.");
            Assert.AreEqual($"Item {i}", read.MyString1, $"Item at index {i} should have correct MyString1.");
        }
    }

    [TestMethod]
    public void ReadSingleSpanningInnerAndBuffer()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        // Pre-populate inner bucket with 50 items
        var innerItems = Enumerable.Range(0, 50).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Inner {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = true,
            MyString2 = $"InnerString {i}"
        }).ToArray();
        persistService.Add(innerItems);

        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add 50 items to buffer
        var bufferItems = Enumerable.Range(50, 50).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Buffer {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = false,
            MyString2 = $"BufferString {i}"
        }).ToArray();
        buffer.Add(bufferItems);

        // Read from inner bucket
        var readInner = buffer.Read(25);
        Assert.AreEqual(25, readInner.MyNumber1);
        Assert.AreEqual("Inner 25", readInner.MyString1);

        // Read from buffer
        var readBuffer = buffer.Read(75);
        Assert.AreEqual(75, readBuffer.MyNumber1);
        Assert.AreEqual("Buffer 75", readBuffer.MyString1);

        // Read boundary (first buffer item)
        var readBoundary = buffer.Read(50);
        Assert.AreEqual(50, readBoundary.MyNumber1);
        Assert.AreEqual("Buffer 50", readBoundary.MyString1);
    }

    [TestMethod]
    public void ReadThrowsForOutOfRangeIndex()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = new[]
        {
            new SampleData
                { MyNumber1 = 1, MyString1 = "Item 1", MyNumber2 = 10, MyBoolean1 = true, MyString2 = "String 1" }
        };
        buffer.Add(items);

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => buffer.Read(-1),
            "Reading negative index should throw.");
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => buffer.Read(100),
            "Reading index beyond count should throw.");
    }

    [TestMethod]
    public async Task ReadSnapshotHandlesLargeBuffer()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = Enumerable.Range(0, 10000).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"LargeItem {i}",
            MyNumber2 = i * 3,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"LargeString {i}"
        }).ToArray();

        buffer.Add(items);

        // Read the first, middle, and last items to verify correctness with large buffer
        var first = buffer.Read(0);
        Assert.AreEqual(0, first.MyNumber1);
        Assert.AreEqual("LargeItem 0", first.MyString1);

        var middle = buffer.Read(5000);
        Assert.AreEqual(5000, middle.MyNumber1);
        Assert.AreEqual("LargeItem 5000", middle.MyString1);

        var last = buffer.Read(9999);
        Assert.AreEqual(9999, last.MyNumber1);
        Assert.AreEqual("LargeItem 9999", last.MyString1);

        // Verify data is still correct after sync
        await buffer.SyncAsync();

        var firstAfterSync = buffer.Read(0);
        Assert.AreEqual(0, firstAfterSync.MyNumber1);
    }

    [TestMethod]
    public void ReadSnapshotConsistencyUnderConcurrentAdds()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Pre-add some items
        var initialItems = Enumerable.Range(0, 50).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Initial {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = true,
            MyString2 = $"InitialString {i}"
        }).ToArray();
        buffer.Add(initialItems);

        // Concurrently read and add from multiple threads
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
                        var item = buffer.Read(i);
                        Assert.AreEqual(i, item.MyNumber1,
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
        Assert.AreEqual(50, buffer.Count);
    }
}
