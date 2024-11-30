using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Aiursoft.ArrayDb.WriteBuffer;
using Aiursoft.ArrayDb.WriteBuffer.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class BufferedObjectBucketsTests : ArrayDbTestBase
{
    [TestMethod]
    public async Task BufferTransitionsFromColdToHot()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        Assert.IsTrue(buffer.IsCold, "Buffer should start in a cold state.");

        var sampleData = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "Hello, World! 你好世界",
            MyNumber2 = 2,
            MyBoolean1 = true,
            MyString2 = "This is a string"
        };

        buffer.Add(sampleData);

        Assert.IsTrue(buffer.IsHot, "Buffer should be hot after adding an item.");

        // Wait for the buffer to cool down naturally
        await WaitForBufferToBecomeCold(buffer);

        // Ensure data is persisted after cooldown
        Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount,
            "Data should be persisted after buffer cools down.");
    }

    [TestMethod]
    public async Task AddingMultipleItemsBuffersCorrectly()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        Assert.IsTrue(buffer.IsCold, "Buffer should start in a cold state.");

        var sampleData = new[]
        {
            new SampleData
            {
                MyNumber1 = 1, MyString1 = "Item 1", MyNumber2 = 2, MyBoolean1 = true, MyString2 = "This is item 1"
            },
            new SampleData
            {
                MyNumber1 = 2, MyString1 = "Item 2", MyNumber2 = 3, MyBoolean1 = false, MyString2 = "This is item 2"
            },
            new SampleData
            {
                MyNumber1 = 3, MyString1 = "Item 3", MyNumber2 = 4, MyBoolean1 = true, MyString2 = "This is item 3"
            }
        };

        buffer.Add(sampleData);

        Assert.IsTrue(buffer.IsHot, "Buffer should be hot after adding multiple items.");

        // Wait for the buffer to cool down naturally
        await WaitForBufferToBecomeCold(buffer);

        // Ensure all data is persisted
        Assert.AreEqual(3, persistService.SpaceProvisionedItemsCount,
            "All items should be persisted after buffer cools down.");
    }

    [TestMethod]
    public async Task BufferStatePersistsUntilSync()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        Assert.IsTrue(buffer.IsCold, "Buffer should start in a cold state.");

        var sampleData = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "Initial Data",
            MyNumber2 = 2,
            MyBoolean1 = true,
            MyString2 = "Testing persistence"
        };

        buffer.Add(sampleData);

        Assert.IsTrue(buffer.IsHot, "Buffer should be hot after adding an item.");
        Assert.AreEqual(1, buffer.Count, "Buffer should contain one item.");
        
        //Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount, "Data should not be persisted immediately.");

        await buffer.SyncAsync();

        Assert.AreEqual(1, buffer.Count, "Buffer should still contain one item after synchronization.");
        Assert.IsTrue(buffer.IsCold, "Buffer should become cold after synchronization.");
        Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount,
            "Data should be persisted after synchronization.");
    }

    private async Task WaitForBufferToBecomeCold(BufferedObjectBuckets<SampleData> buffer)
    {
        for (int i = 0; i < 30; i++) // Max wait time ~3 seconds
        {
            if (buffer.IsCold) break;
            await Task.Delay(100);
        }

        Assert.IsTrue(buffer.IsCold, "Buffer did not become cold within the expected time.");
    }

    [TestMethod]
    public async Task MultipleThreadsAddingDataConcurrently()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);
        Assert.IsTrue(buffer.IsCold, "Buffer should start in a cold state.");

        var tasks = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            var i1 = i;
            tasks.Add(Task.Run(() =>
            {
                var sampleData = new SampleData
                {
                    MyNumber1 = i1,
                    MyString1 = $"Data {i1}",
                    MyNumber2 = i1 + 1,
                    MyBoolean1 = i1 % 2 == 0,
                    MyString2 = $"Sample {i1}"
                };
                buffer.Add(sampleData);
            }));
        }

        await Task.WhenAll(tasks);
        await buffer.SyncAsync();

        // Verify all data is persisted
        Assert.AreEqual(10, persistService.SpaceProvisionedItemsCount,
            "All data added by multiple threads should be persisted.");
    }

    [TestMethod]
    public async Task BufferHandlesRapidAdditionsGracefully()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        Assert.IsTrue(buffer.IsCold, "Buffer should start in a cold state.");

        // Rapidly add a large number of items
        for (int i = 0; i < 1000; i++)
        {
            var sampleData = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Data {i}",
                MyNumber2 = i + 1,
                MyBoolean1 = true,
                MyString2 = $"Item {i}"
            };
            buffer.Add(sampleData);
        }

        await buffer.SyncAsync();

        // Verify all items are persisted
        Assert.AreEqual(1000, persistService.SpaceProvisionedItemsCount,
            "All rapidly added items should be persisted.");
    }

    [TestMethod]
    public void BufferDoesNotCrashOnEmptyAddBufferedCall()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Call AddBuffered with no data
        buffer.Add();

        Assert.IsTrue(buffer.IsCold, "Buffer should remain cold after adding no items.");
    }

    [TestMethod]
    public async Task SyncAsyncWaitsForAllPendingWrites()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        for (int i = 0; i < 5; i++)
        {
            var sampleData = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Data {i}",
                MyNumber2 = i + 1,
                MyBoolean1 = true,
                MyString2 = $"Item {i}"
            };
            buffer.Add(sampleData);
        }

        // Call SyncAsync and ensure it waits for all data to be written
        await buffer.SyncAsync();

        Assert.IsTrue(buffer.IsCold, "Buffer should be cold after SyncAsync completes.");
        Assert.AreEqual(5, persistService.SpaceProvisionedItemsCount,
            "All items should be persisted after SyncAsync.");
    }

    [TestMethod]
    public void CalculateSleepTimeReturnsZeroForLargeBuffer()
    {
        int result = TimeExtensions.CalculateSleepTime(1000, 10, 20);
        Assert.AreEqual(0, result, "Sleep time should be 0 when buffer size exceeds threshold.");
    }

    [TestMethod]
    public void CalculateSleepTimeHandlesSmallBufferSizes()
    {
        int result = TimeExtensions.CalculateSleepTime(1000, 10, 2);
        Assert.IsTrue(result is > 0 and <= 1000,
            "Sleep time should be within valid range for small buffer sizes.");
    }

    [TestMethod]
    [ExpectedException(typeof(ArgumentException))]
    public void CalculateSleepTimeThrowsForInvalidThreshold()
    {
        TimeExtensions.CalculateSleepTime(1000, -1, 5);
    }

    [TestMethod]
    public async Task SyncAsyncCoversEngineRunningScenario()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add initial data
        for (int i = 0; i < 5; i++)
        {
            buffer.Add(new SampleData
            {
                MyNumber1 = i, MyString1 = $"Data {i}", MyNumber2 = i * 10, MyBoolean1 = true,
                MyString2 = $"Sample {i}"
            });
        }

        // Trigger engine to start writing asynchronously
        await Task.Delay(50);

        // Call SyncAsync while the engine is potentially still working
        await buffer.SyncAsync();

        // Ensure all data is persisted after SyncAsync completes
        Assert.AreEqual(5, persistService.SpaceProvisionedItemsCount,
            "All items should be persisted after SyncAsync completes.");
    }

    [TestMethod]
    public async Task SyncAsyncCoversCoolDownPhaseScenario()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add initial data and trigger write
        buffer.Add(new SampleData
            { MyNumber1 = 1, MyString1 = "Initial Data", MyNumber2 = 10, MyBoolean1 = true, MyString2 = "Data" });

        // Wait for the buffer to cool down
        await WaitForBufferToBecomeCold(buffer);

        // Call SyncAsync while the engine is in the cool-down phase
        await buffer.SyncAsync();

        // Ensure data is persisted
        Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount,
            "Data should be persisted after SyncAsync even if the buffer was in cool-down phase.");
    }

    [TestMethod]
    public async Task SyncAsyncHandlesMultipleRoundsOfData()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add a batch of data
        for (int i = 0; i < 10; i++)
        {
            buffer.Add(new SampleData
            {
                MyNumber1 = i, MyString1 = $"Batch 1 Data {i}", MyNumber2 = i * 2, MyBoolean1 = false,
                MyString2 = $"Batch 1 {i}"
            });
        }

        // Allow some data to be processed but not all
        await Task.Delay(100);

        // Add more data during the writing process
        for (int i = 0; i < 5; i++)
        {
            buffer.Add(new SampleData
            {
                MyNumber1 = i + 10, MyString1 = $"Batch 2 Data {i}", MyNumber2 = i * 3, MyBoolean1 = true,
                MyString2 = $"Batch 2 {i}"
            });
        }

        // Call SyncAsync to ensure all data is written
        await buffer.SyncAsync();

        // Verify all data is persisted
        Assert.AreEqual(15, persistService.SpaceProvisionedItemsCount,
            "All items across multiple batches should be persisted after SyncAsync.");
    }


    [TestMethod]
    public async Task BufferContainsItemsBeforePersistence()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = Enumerable.Range(1, 100000).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        var tasks = items.Select(item => Task.Factory.StartNew(() => buffer.Add(item)));
        await Task.WhenAll(tasks);

        for (var i = 0; i < 100; i++)
        {
            Assert.AreEqual(100000, buffer.Count,
                "Buffer should contain all items before persistence starts.");
            await Task.Delay(10);
        }
    }

    [TestMethod]
    public async Task BufferPartiallyPersistsDataCorrectly()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = Enumerable.Range(1, 50000).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        buffer.Add(items);

        // Wait briefly to allow some data to persist
        for (var i = 0; i < 250; i++)
        {
            Assert.AreEqual(buffer.Count, 50000, "No matter how much data is persisted, buffer should contain all items.");
            await Task.Delay(10); // Adjust time based on your buffer persistence settings
        }
        Assert.IsTrue(buffer.BufferedItemsCount < 50000, "Some data should have been persisted.");
        Assert.IsTrue(persistService.Count > 0, "Some data should be in the underlying storage.");
    }

    [TestMethod]
    public async Task BufferBecomesEmptyAfterCompletePersistence()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = Enumerable.Range(1, 100000).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        buffer.Add(items);

        var readBeforeBufferPersist = buffer.Read(90000);
        Assert.AreEqual(items[90000].MyNumber1, readBeforeBufferPersist.MyNumber1,
            "Data should be readable from buffer before persistence.");

        // Wait for all items to be persisted
        await buffer.SyncAsync();

        Assert.AreEqual(0, buffer.BufferedItemsCount, "Buffer should be empty after complete persistence.");
        Assert.AreEqual(100000, persistService.Count, "All data should be persisted in the underlying storage.");
    }

    [TestMethod]
    public async Task BufferedDataReadCorrectlyBeforeAndAfterPersistence()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = new[]
        {
            new SampleData
                { MyNumber1 = 1, MyString1 = "Item 1", MyNumber2 = 10, MyBoolean1 = true, MyString2 = "String 1" },
            new SampleData
                { MyNumber1 = 2, MyString1 = "Item 2", MyNumber2 = 20, MyBoolean1 = false, MyString2 = "String 2" }
        };

        buffer.Add(items);

        var readItemBeforePersist = buffer.Read(0);
        Assert.AreEqual(items[0].MyNumber1, readItemBeforePersist.MyNumber1,
            "Data should be readable from buffer before persistence.");

        // Allow data to be persisted
        await buffer.SyncAsync();

        var readItemAfterPersist = buffer.Read(0);
        Assert.AreEqual(items[0].MyNumber1, readItemAfterPersist.MyNumber1,
            "Data should be readable from underlying storage after persistence.");
    }

    [TestMethod]
    public void ReadBulkFromBufferOnly()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        var items = Enumerable.Range(1, 100).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Buffered Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"Buffered String {i}"
        }).ToArray();

        buffer.Add(items);

        // Read bulk data directly from the buffer before persistence
        var bulkRead = buffer.ReadBulk(0, 10);
        Assert.AreEqual(10, bulkRead.Length, "Should return 10 items from buffer.");
        Assert.AreEqual("Buffered Item 1", bulkRead[0].MyString1,
            "First item should match the expected value from the buffer.");
    }

    [TestMethod]
    public void ReadBulkSpanningBufferAndPersistedStorage()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Persisted data
        var persistedItems = Enumerable.Range(1, 50).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Persisted Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"Persisted String {i}"
        }).ToArray();
        persistService.Add(persistedItems);

        // Buffer data
        var bufferedItems = Enumerable.Range(51, 50).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Buffered Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"Buffered String {i}"
        }).ToArray();
        buffer.Add(bufferedItems);

        // Read bulk that spans both persisted and buffered data
        var bulkRead = buffer.ReadBulk(40, 20);
        Assert.AreEqual(20, bulkRead.Length, "Should return 20 items spanning persisted and buffered data.");
        Assert.AreEqual("Persisted Item 41", bulkRead[0].MyString1,
            "First item should be from persisted data.");
        Assert.AreEqual("Buffered Item 51", bulkRead[10].MyString1, "11th item should be from buffered data.");
    }

    [TestMethod]
    public void ReadBulkExceedingAvailableData()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add a few items
        var items = Enumerable.Range(1, 5).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        buffer.Add(items);

        // Read more items than available
        try
        {
            buffer.ReadBulk(0, 10);
            Assert.Fail("Reading bulk exceeding available data should throw an exception.");
        }
        catch (ArgumentException)
        {
        }
        
        // Read more items than available
        try
        {
            buffer.ReadBulk(3, 3);
            Assert.Fail("Reading bulk exceeding available data should throw an exception.");
        }
        catch (ArgumentException)
        {
        }
        
        try
        {
            buffer.ReadBulk(3, -1);
            Assert.Fail("Reading bulk exceeding available data should throw an exception.");
        }
        catch (ArgumentException)
        {
        }
        
        try
        {
            buffer.ReadBulk(6, 1);
            Assert.Fail("Reading bulk exceeding available data should throw an exception.");
        }
        catch (ArgumentException)
        {
        }
    }

    [TestMethod]
    public async Task ReadBulkAfterCompletePersistence()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add items and ensure they are persisted
        var items = Enumerable.Range(1, 100).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        buffer.Add(items);
        await buffer.SyncAsync();

        // Read bulk from persisted data
        var bulkRead = buffer.ReadBulk(0, 20);
        Assert.AreEqual(20, bulkRead.Length, "Should return 20 items from persisted storage.");
        Assert.AreEqual("Item 1", bulkRead[0].MyString1, "First item should match the expected value.");
        Assert.AreEqual("Item 20", bulkRead[19].MyString1, "Last item should match the expected value.");
    }

    [TestMethod]
    public async Task ReadBulkWithOffset()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);

        // Add items
        var items = Enumerable.Range(0, 30000).Select(i => new SampleData
        {
            MyNumber1 = i,
            MyString1 = $"Item {i}",
            MyNumber2 = i * 2,
            MyBoolean1 = i % 2 == 0,
            MyString2 = $"String {i}"
        }).ToArray();

        buffer.Add(items);

        // Read bulk with an offset
        var bulkRead = buffer.ReadBulk(100, 10000);
        Assert.AreEqual(10000, bulkRead.Length, "Should return 10000 items starting from offset.");
        Assert.AreEqual("Item 100", bulkRead[0].MyString1, "First item should start at the correct offset.");
        
        await buffer.SyncAsync();
    }
}