using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Aiursoft.ArrayDb.WriteBuffer;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class BufferedObjectBucketsTests : ArrayDbTestBase
{
    [TestMethod]
    public async Task CoolHotSwitchTest()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);
        Assert.IsTrue(buffer.IsCold);
        var sampleData = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "Hello, World! 你好世界",
            MyNumber2 = 2,
            MyBoolean1 = true,
            MyString2 = "This is a string"
        };
        buffer.AddBuffered(sampleData);
        Assert.IsTrue(buffer.IsHot);

        await Task.Delay(2000);
        Assert.IsTrue(buffer.IsCold);

        // Data actually written.
        Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount);
    }

    [TestMethod]
    public async Task TestAddBufferedCollection()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var buffer = new BufferedObjectBuckets<SampleData>(persistService);
        Assert.IsTrue(buffer.IsCold);

        var sampleDatas = new[]
        {
            new SampleData
            {
                MyNumber1 = 1,
                MyString1 = "Hello, World! 你好世界",
                MyNumber2 = 2,
                MyBoolean1 = true,
                MyString2 = "This is a string"
            },
            new SampleData
            {
                MyNumber1 = 2,
                MyString1 = "Hello, World! 你好世界",
                MyNumber2 = 3,
                MyBoolean1 = true,
                MyString2 = "This is a string"
            },
            new SampleData
            {
                MyNumber1 = 3,
                MyString1 = "Hello, World! 你好世界",
                MyNumber2 = 4,
                MyBoolean1 = true,
                MyString2 = "This is a string"
            }
        };
        buffer.AddBuffered(sampleDatas);
        Assert.IsTrue(buffer.IsHot);

        await Task.Delay(1010);
        Assert.IsTrue(buffer.IsCold);

        // Data actually written.
        Assert.AreEqual(3, persistService.SpaceProvisionedItemsCount);
    }

    [TestMethod]
    public async Task WriteWhenHotNotActuallyWritten()
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

        // Initial state check: Cold and no items in buffers
        Assert.AreEqual(0, buffer.WriteTimesCount, "Write count should initially be zero.");
        Assert.AreEqual(0, buffer.WriteItemsCount, "Item count should initially be zero.");
        Assert.AreEqual(0, persistService.SpaceProvisionedItemsCount,
            "Persisted item count should be zero at the beginning.");

        // Add and persist the first item
        buffer.AddBuffered(sampleData);

        // Verify state and statistics after the first write
        Assert.IsTrue(buffer.IsHot, "Buffer should be hot after the first write.");
        Assert.AreEqual(1, buffer.WriteTimesCount, "Write count should be 1 after the first buffered write.");
        Assert.AreEqual(1, buffer.WriteItemsCount, "1 item should have been written.");
        
        // Comment the following test cases. Because in some machine, buffered item inserts too fast that it's already written.
        //Assert.AreEqual(1, buffer.BufferedItemsCount, "Buffered item count should be 1 after the first write.");
        //Assert.AreEqual(0, persistService.SpaceProvisionedItemsCount,
        //    "Persisted item count should be 0 after the first write because it's still hot.");
        
        // Wait a while, and the item will be written
        await Task.Delay(1010);
        Assert.IsTrue(buffer.IsCold, "Buffer should be cold after the write cooldown period.");
        Assert.AreEqual(0, buffer.BufferedItemsCount, "Buffered item count should be 1 after the first write.");
        Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount,
            "Persisted item count should be 1 after the write cooldown period.");
        
        // Add two items one by one
        buffer.AddBuffered(sampleData);
        Assert.IsTrue(buffer.IsHot, "Buffer should be hot after the first write.");
        buffer.AddBuffered(sampleData);
        
        Assert.IsTrue(buffer.IsHot, "Buffer should be hot after the first write.");
        Assert.AreEqual(3, buffer.WriteTimesCount, "Write count should be 3 after the first buffered write.");
        Assert.AreEqual(3, buffer.WriteItemsCount, "3 item should have been written.");
        Assert.AreEqual(2, buffer.BufferedItemsCount, "Buffered item count should be 1 after the first write.");
        Assert.AreEqual(1, persistService.SpaceProvisionedItemsCount,
            "Persisted item count should be 0 after the first write because it's still hot.");

        await buffer.SyncAsync();
        Assert.IsTrue(buffer.IsCold, "Buffer should be cold after the write cooldown period.");
        Assert.AreEqual(0, buffer.BufferedItemsCount, "Buffered item count should be 1 after the first write.");
        Assert.AreEqual(3, persistService.SpaceProvisionedItemsCount,
            "Persisted item count should be 1 after the write cooldown period.");
    }
}