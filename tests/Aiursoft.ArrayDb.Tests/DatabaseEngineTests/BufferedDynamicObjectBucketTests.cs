using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;
using System.Text;
using Aiursoft.ArrayDb.WriteBuffer.Dynamic;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class BufferedDynamicObjectBucketTests : ArrayDbTestBase
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
                        Value = $"Hello, World! 你好世界 {i}"
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
    public async Task BufferTransitionsFromColdToHot()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should start in a cold state.");

        var sampleItem = CreateSampleItem(1);

        bufferedBucket.Add(sampleItem);

        Assert.IsFalse(bufferedBucket.IsCold, "Buffer should be hot after adding an item.");

        // Wait for the buffer to cool down naturally
        await WaitForBufferToBecomeCold(bufferedBucket);

        // Ensure data is persisted after cooldown
        Assert.AreEqual(1, dynamicBucket.SpaceProvisionedItemsCount,
            "Data should be persisted after buffer cools down.");
    }

    [TestMethod]
    public async Task AddingMultipleItemsBuffersCorrectly()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should start in a cold state.");

        var items = new List<BucketItem>();
        for (var i = 0; i < 3; i++)
        {
            items.Add(CreateSampleItem(i));
        }

        bufferedBucket.Add(items.ToArray());

        Assert.IsFalse(bufferedBucket.IsCold, "Buffer should be hot after adding multiple items.");

        // Wait for the buffer to cool down naturally
        await WaitForBufferToBecomeCold(bufferedBucket);

        // Ensure all data is persisted
        Assert.AreEqual(3, dynamicBucket.SpaceProvisionedItemsCount,
            "All items should be persisted after buffer cools down.");
    }

    [TestMethod]
    public async Task BufferStatePersistsUntilSync()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should start in a cold state.");

        var sampleItem = CreateSampleItem(1);

        bufferedBucket.Add(sampleItem);

        Assert.IsFalse(bufferedBucket.IsCold, "Buffer should be hot after adding an item.");
        Assert.AreEqual(1, bufferedBucket.Count, "Buffer should contain one item.");

        await bufferedBucket.SyncAsync();

        Assert.AreEqual(1, bufferedBucket.Count, "Buffer should still contain one item after synchronization.");
        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should become cold after synchronization.");
        Assert.AreEqual(1, dynamicBucket.SpaceProvisionedItemsCount,
            "Data should be persisted after synchronization.");
    }

    [TestMethod]
    public async Task MultipleThreadsAddingDataConcurrently()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should start in a cold state.");

        var tasks = new List<Task>();
        for (var i = 0; i < 10; i++)
        {
            var i1 = i;
            tasks.Add(Task.Run(() =>
            {
                var sampleItem = CreateSampleItem(i1);
                bufferedBucket.Add(sampleItem);
            }));
        }

        await Task.WhenAll(tasks);
        await bufferedBucket.SyncAsync();

        // Verify all data is persisted
        Assert.AreEqual(10, dynamicBucket.SpaceProvisionedItemsCount,
            "All data added by multiple threads should be persisted.");
    }

    [TestMethod]
    public async Task BufferHandlesRapidAdditionsGracefully()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should start in a cold state.");

        // Rapidly add a large number of items
        for (var i = 0; i < 1000; i++)
        {
            var sampleItem = CreateSampleItem(i);
            bufferedBucket.Add(sampleItem);
        }

        await bufferedBucket.SyncAsync();

        // Verify all items are persisted
        Assert.AreEqual(1000, dynamicBucket.SpaceProvisionedItemsCount,
            "All rapidly added items should be persisted.");
    }

    [TestMethod]
    public void BufferDoesNotCrashOnEmptyAddCall()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Call Add with no data
        bufferedBucket.Add();

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should remain cold after adding no items.");
    }

    [TestMethod]
    public async Task SyncAsyncWaitsForAllPendingWrites()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        for (var i = 0; i < 5; i++)
        {
            var sampleItem = CreateSampleItem(i);
            bufferedBucket.Add(sampleItem);
        }

        // Call SyncAsync and ensure it waits for all data to be written
        await bufferedBucket.SyncAsync();

        Assert.IsTrue(bufferedBucket.IsCold, "Buffer should be cold after SyncAsync completes.");
        Assert.AreEqual(5, dynamicBucket.SpaceProvisionedItemsCount,
            "All items should be persisted after SyncAsync.");
    }

    [TestMethod]
    public async Task SyncAsyncCoversEngineRunningScenario()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add initial data
        for (var i = 0; i < 5; i++)
        {
            bufferedBucket.Add(CreateSampleItem(i));
        }

        // Trigger engine to start writing asynchronously
        await Task.Delay(50);

        // Call SyncAsync while the engine is potentially still working
        await bufferedBucket.SyncAsync();

        // Ensure all data is persisted after SyncAsync completes
        Assert.AreEqual(5, dynamicBucket.SpaceProvisionedItemsCount,
            "All items should be persisted after SyncAsync completes.");
    }

    [TestMethod]
    public async Task SyncAsyncCoversCoolDownPhaseScenario()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add initial data and trigger write
        bufferedBucket.Add(CreateSampleItem(1));

        // Wait for the buffer to cool down
        await WaitForBufferToBecomeCold(bufferedBucket);

        // Call SyncAsync while the engine is in the cool-down phase
        await bufferedBucket.SyncAsync();

        // Ensure data is persisted
        Assert.AreEqual(1, dynamicBucket.SpaceProvisionedItemsCount,
            "Data should be persisted after SyncAsync even if the buffer was in cool-down phase.");
    }

    [TestMethod]
    public async Task SyncAsyncHandlesMultipleRoundsOfData()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add a batch of data
        for (var i = 0; i < 10; i++)
        {
            bufferedBucket.Add(CreateSampleItem(i));
        }

        // Allow some data to be processed but not all
        await Task.Delay(100);

        // Add more data during the writing process
        for (var i = 10; i < 15; i++)
        {
            bufferedBucket.Add(CreateSampleItem(i));
        }

        // Call SyncAsync to ensure all data is written
        await bufferedBucket.SyncAsync();

        // Verify all data is persisted
        Assert.AreEqual(15, dynamicBucket.SpaceProvisionedItemsCount,
            "All items across multiple batches should be persisted after SyncAsync.");
    }

    [TestMethod]
    public async Task BufferBecomesEmptyAfterCompletePersistence()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        var items = new List<BucketItem>();
        for (var i = 0; i < 1000; i++)
        {
            items.Add(CreateSampleItem(i));
        }

        bufferedBucket.Add(items.ToArray());

        var readBeforePersist = bufferedBucket.Read(900);
        Assert.AreEqual(900, readBeforePersist.Properties["MyNumber1"].Value,
            "Data should be readable from buffer before persistence.");

        // Wait for all items to be persisted
        await bufferedBucket.SyncAsync();

        Assert.AreEqual(0, bufferedBucket.BufferedItemsCount,
            "Buffer should be empty after complete persistence.");
        Assert.AreEqual(1000, dynamicBucket.Count,
            "All data should be persisted in the underlying storage.");
    }

    [TestMethod]
    public async Task BufferedDataReadCorrectlyBeforeAndAfterPersistence()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        var items = new[]
        {
            CreateSampleItem(1),
            CreateSampleItem(2)
        };

        bufferedBucket.Add(items);

        var readItemBeforePersist = bufferedBucket.Read(0);
        Assert.AreEqual(items[0].Properties["MyNumber1"].Value, readItemBeforePersist.Properties["MyNumber1"].Value,
            "Data should be readable from buffer before persistence.");

        // Allow data to be persisted
        await bufferedBucket.SyncAsync();

        var readItemAfterPersist = bufferedBucket.Read(0);
        Assert.AreEqual(items[0].Properties["MyNumber1"].Value, readItemAfterPersist.Properties["MyNumber1"].Value,
            "Data should be readable from underlying storage after persistence.");
    }

    [TestMethod]
    public void ReadBulkFromBufferOnly()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        var items = new List<BucketItem>();
        for (var i = 1; i <= 100; i++)
        {
            items.Add(CreateSampleItem(i));
        }

        bufferedBucket.Add(items.ToArray());

        // Read bulk data directly from the buffer before persistence
        var bulkRead = bufferedBucket.ReadBulk(0, 10);
        Assert.AreEqual(10, bulkRead.Length, "Should return 10 items from buffer.");
        Assert.AreEqual(1, bulkRead[0].Properties["MyNumber1"].Value,
            "First item should match the expected value from the buffer.");
    }

    [TestMethod]
    public void ReadBulkSpanningBufferAndPersistedStorage()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);

        // Persisted data
        var persistedItems = new List<BucketItem>();
        for (var i = 1; i <= 50; i++)
        {
            persistedItems.Add(CreateSampleItem(i));
        }
        dynamicBucket.Add(persistedItems.ToArray());

        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Buffer data
        var bufferedItems = new List<BucketItem>();
        for (var i = 51; i <= 100; i++)
        {
            bufferedItems.Add(CreateSampleItem(i));
        }
        bufferedBucket.Add(bufferedItems.ToArray());

        // Read bulk that spans both persisted and buffered data
        var bulkRead = bufferedBucket.ReadBulk(40, 20);
        Assert.AreEqual(20, bulkRead.Length, "Should return 20 items spanning persisted and buffered data.");
        Assert.AreEqual(41, bulkRead[0].Properties["MyNumber1"].Value,
            "First item should be from persisted data.");
        Assert.AreEqual(51, bulkRead[10].Properties["MyNumber1"].Value, "11th item should be from buffered data.");
    }

    [TestMethod]
    public void ReadBulkExceedingAvailableData()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add a few items
        var items = new List<BucketItem>();
        for (var i = 1; i <= 5; i++)
        {
            items.Add(CreateSampleItem(i));
        }
        bufferedBucket.Add(items.ToArray());

        // Read more items than available
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => bufferedBucket.ReadBulk(0, 10),
            "Reading bulk exceeding available data should throw an exception.");

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => bufferedBucket.ReadBulk(3, 3),
            "Reading bulk exceeding available data should throw an exception.");

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => bufferedBucket.ReadBulk(3, -1),
            "Reading bulk exceeding available data should throw an exception.");

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() => bufferedBucket.ReadBulk(6, 1),
            "Reading bulk exceeding available data should throw an exception.");
    }

    [TestMethod]
    public async Task ReadBulkAfterCompletePersistence()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add items and ensure they are persisted
        var items = new List<BucketItem>();
        for (var i = 1; i <= 100; i++)
        {
            items.Add(CreateSampleItem(i));
        }
        bufferedBucket.Add(items.ToArray());
        await bufferedBucket.SyncAsync();

        // Read bulk from persisted data
        var bulkRead = bufferedBucket.ReadBulk(0, 20);
        Assert.AreEqual(20, bulkRead.Length, "Should return 20 items from persisted storage.");
        Assert.AreEqual(1, bulkRead[0].Properties["MyNumber1"].Value, "First item should match the expected value.");
        Assert.AreEqual(20, bulkRead[19].Properties["MyNumber1"].Value, "Last item should match the expected value.");
    }

    [TestMethod]
    public async Task ReadBulkWithOffset()
    {
        var typeDefinition = GetSampleTypeDefinition();
        var dynamicBucket = new DynamicObjectBucket(typeDefinition, TestFilePath, TestFilePathStrings);
        var bufferedBucket = new BufferedDynamicObjectBucket(dynamicBucket);

        // Add items
        var items = new List<BucketItem>();
        for (var i = 0; i < 30000; i++)
        {
            items.Add(CreateSampleItem(i));
        }
        bufferedBucket.Add(items.ToArray());

        // Read bulk with an offset
        var bulkRead = bufferedBucket.ReadBulk(100, 10000);
        Assert.AreEqual(10000, bulkRead.Length, "Should return 10000 items starting from offset.");
        Assert.AreEqual(100, bulkRead[0].Properties["MyNumber1"].Value, "First item should start at the correct offset.");

        await bufferedBucket.SyncAsync();
    }

    private async Task WaitForBufferToBecomeCold(BufferedDynamicObjectBucket buffer)
    {
        for (var i = 0; i < 30; i++) // Max wait time ~3 seconds
        {
            if (buffer.IsCold) break;
            await Task.Delay(100);
        }

        Assert.IsTrue(buffer.IsCold, "Buffer did not become cold within the expected time.");
    }
}
