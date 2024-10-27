using Aiursoft.ArrayDb.Engine;
using Aiursoft.ArrayDb.Tests.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public class BufferedObjectBucketsTests : ArrayDbTestBase
{
    [TestMethod]
    public async Task CoolHotSwitchTest()
    {
        var persistService = new ObjectBuckets<SampleData>("sampleData.bin", "sampleDataStrings.bin");
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
        await buffer.AddAsync(sampleData);
        Assert.IsTrue(buffer.IsHot);
        
        await Task.Delay(1010);
        Assert.IsTrue(buffer.IsCold);
        
        // Data actually written.
        Assert.AreEqual(1, persistService.Count);
    }

    [TestMethod]
    public async Task WriteWhenHotNotActuallyWritten()
    {
        var persistService = new ObjectBuckets<SampleData>("sampleData.bin", "sampleDataStrings.bin");
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
        
        // Initial state: Cold and nothing inside.
        Assert.IsTrue(buffer.IsCold);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.AreEqual(0, persistService.Count);
        
        // Add something. (This is the first write. It's low latency. So it's persisted immediately.)
        await buffer.AddAsync(sampleData);
        await buffer.WaitUntilWriteCompleteAsync();
        
        // It's hot, however the first write is persisted immediately.
        Assert.IsTrue(buffer.IsHot);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.AreEqual(1, persistService.Count);

        // Add again. (This is the second write. It's high latency. So it's buffered.)
        await buffer.AddAsync(sampleData);
        
        // It's hot, and the second write is buffered.
        Assert.IsTrue(buffer.IsHot);
        Assert.AreEqual(1, buffer.BufferedItemsCount);
        Assert.AreEqual(1, persistService.Count);
        
        // Wait until the cooldown. The buffered items should be persisted. However, since it just persisted an item, it's still hot. 
        await Task.Delay(1010);
        Assert.IsTrue(buffer.IsHot);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.AreEqual(2, persistService.Count);

        // Wait until the cooldown. Now it's cold.
        await Task.Delay(1010);
        Assert.IsTrue(buffer.IsCold);
        Assert.AreEqual(0, buffer.BufferedItemsCount);
        Assert.AreEqual(2, persistService.Count);
    }
}