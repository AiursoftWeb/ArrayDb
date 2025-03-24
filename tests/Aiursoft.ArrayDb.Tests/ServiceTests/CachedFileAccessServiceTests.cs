using System.Diagnostics.CodeAnalysis;
using Aiursoft.ArrayDb.ReadLruCache;
using Aiursoft.ArrayDb.Tests.Base;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests.ServiceTests;

[TestClass]
[DoNotParallelize]
public class CachedFileAccessServiceTests : ArrayDbTestBase
{
    private const long InitialSize = 10 * 1024 * 1024; // 10 MB
    private const int PageSize = 0x100000; // 1 MB

    [NotNull]
    // ReSharper disable once RedundantDefaultMemberInitializer
    private CachedFileAccessService? _service = null!;

    [TestInitialize]
    public void SetUp()
    {
        // Delete the test file before each test
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }

        // Initialize the CachedFileAccessService
        _service = new CachedFileAccessService(TestFilePath, 
            initialUnderlyingFileSizeIfNotExists: InitialSize, 
            cachePageSize: PageSize, 
            maxCachedPagesCount: 512,
            hotCacheItems: 16);
    }

    [TestCleanup]
    public void TearDown()
    {
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }
    }

    [TestMethod]
    public void TestReadAndWriteSinglePage()
    {
        var dataToWrite = "Hello, Cache!"u8.ToArray();

        // Write data to the cache
        _service.WriteInFile(0, dataToWrite);

        // Read back the data
        var readData = _service.ReadInFile(0, dataToWrite.Length);

        // Verify the written and read data match
        CollectionAssert.AreEqual(dataToWrite, readData);

        Assert.AreEqual(1, _service.CacheMissCount);
        Assert.AreEqual(0, _service.CacheHitCount);
    }

    [TestMethod]
    public void TestReadAndWriteMultiplePages()
    {
        var dataToWrite = new byte[PageSize * 2];
        new Random().NextBytes(dataToWrite);

        // Write data spanning across two pages
        _service.WriteInFile(0, dataToWrite);

        // Read back the data
        var readData = _service.ReadInFile(0, dataToWrite.Length);

        // Verify the written and read data match
        CollectionAssert.AreEqual(dataToWrite, readData);

        Assert.AreEqual(2, _service.CacheMissCount);
    }

    [TestMethod]
    public void TestCacheHitAndMissCounts()
    {
        var dataToWrite = new byte[PageSize];
        new Random().NextBytes(dataToWrite);

        // First access should be a miss and load data into the cache
        _service.WriteInFile(0, dataToWrite);
        _service.ReadInFile(0, dataToWrite.Length);

        // Second access should be a hit
        _service.ReadInFile(0, dataToWrite.Length);

        Assert.AreEqual(1, _service.CacheMissCount);
        Assert.AreEqual(1, _service.CacheHitCount);
    }

    [TestMethod]
    public void TestCacheEvictionPolicy()
    {
        var dataToWrite = new byte[PageSize];
        new Random().NextBytes(dataToWrite);

        // Load maxCacheItems + 1 pages to trigger eviction
        for (var i = 0; i < 513; i++)
        {
            _service.WriteInFile(i * PageSize, dataToWrite);
            _service.ReadInFile(i * PageSize, dataToWrite.Length);
        }

        Assert.AreEqual(1, _service.RemoveFromCacheCount);
        Assert.AreEqual(513, _service.CacheMissCount);
    }

    [TestMethod]
    public void TestResetStatistics()
    {
        // Perform some operations
        _service.WriteInFile(0, new byte[PageSize]);
        _service.ReadInFile(0, PageSize);

        // Reset statistics
        _service.ResetAllStatistics();

        Assert.AreEqual(0, _service.CacheHitCount);
        Assert.AreEqual(0, _service.CacheMissCount);
        Assert.AreEqual(0, _service.LruUpdateCount);
        Assert.AreEqual(0, _service.RemoveFromCacheCount);
    }

    [TestMethod]
    public void TestShouldUpdateLru()
    {
        // Load 17 pages to surpass the LRU threshold.
        for (long i = 0; i < 17; i++)
        {
            _service.ReadInFile(i * PageSize, PageSize);
        }

        // Now, access the first page again to trigger an update.
        _service.ReadInFile(0, PageSize);
        Assert.AreEqual(1, _service.LruUpdateCount);
    }

    [TestMethod]
    public void TestStatisticsWithMultipleOperations()
    {
        var dataToWrite = new byte[PageSize];
        new Random().NextBytes(dataToWrite);

        // Write and read multiple pages
        for (var i = 0; i < 10; i++)
        {
            _service.WriteInFile(i * PageSize, dataToWrite);
            _service.ReadInFile(i * PageSize, dataToWrite.Length);
            _service.ReadInFile(i * PageSize, dataToWrite.Length); // Should hit the cache
            _service.ReadInFile(i * PageSize, dataToWrite.Length); // Should hit the cache again
        }

        Assert.AreEqual(10, _service.CacheMissCount);
        Assert.AreEqual(20, _service.CacheHitCount);
    }
}
