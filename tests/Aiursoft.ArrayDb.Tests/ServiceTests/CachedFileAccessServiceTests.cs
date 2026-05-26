using System.Diagnostics.CodeAnalysis;
using Aiursoft.ArrayDb.ReadLruCache;
using Aiursoft.ArrayDb.Tests.Base;

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
    public void TestConcurrentReadsFromDifferentPages()
    {
        // Pre-populate multiple pages with data
        const int pageCount = 32;
        var expected = new byte[pageCount][];
        for (var i = 0; i < pageCount; i++)
        {
            expected[i] = new byte[PageSize];
            new Random(i).NextBytes(expected[i]);
            _service.WriteInFile(i * (long)PageSize, expected[i]);
        }

        // Reset stats
        _service.ResetAllStatistics();

        // Create a new service so the cache is cold and all reads will be misses
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }
        // Re-write with cold service
        _service = new CachedFileAccessService(TestFilePath,
            initialUnderlyingFileSizeIfNotExists: InitialSize,
            cachePageSize: PageSize,
            maxCachedPagesCount: 512,
            hotCacheItems: 16);
        for (var i = 0; i < pageCount; i++)
        {
            _service.WriteInFile(i * (long)PageSize, expected[i]);
        }
        _service.ResetAllStatistics();

        // Concurrent reads from different pages — miss path should not block hit path
        const int threadCount = 8;
        var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (var t = 0; t < threadCount; t++)
        {
            var threadId = t;
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                var pagesPerThread = pageCount / threadCount;
                for (var i = 0; i < pagesPerThread; i++)
                {
                    var pageIndex = threadId * pagesPerThread + i;
                    var result = _service.ReadInFile(pageIndex * (long)PageSize, PageSize);
                    CollectionAssert.AreEqual(
                        expected[pageIndex], result,
                        $"Data mismatch on page {pageIndex} from thread {threadId}");
                }
            });
        }

        Task.WaitAll(tasks);

        // All pages should be loaded exactly once (no duplicates due to double-check)
        Assert.IsTrue(_service.CacheMissCount >= pageCount,
            $"Expected at least {pageCount} misses (one per page), got {_service.CacheMissCount}");
    }

    [TestMethod]
    public void TestConcurrentReadsSamePageDoesNotDoubleLoad()
    {
        // Write one page
        var data = new byte[PageSize];
        new Random(42).NextBytes(data);
        _service.WriteInFile(0, data);

        // Create cold service
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }
        _service = new CachedFileAccessService(TestFilePath,
            initialUnderlyingFileSizeIfNotExists: InitialSize,
            cachePageSize: PageSize,
            maxCachedPagesCount: 512,
            hotCacheItems: 16);
        _service.WriteInFile(0, data);
        _service.ResetAllStatistics();

        // Let one thread warm the cache first
        _service.ReadInFile(0, PageSize);
        Assert.AreEqual(1, _service.CacheMissCount);

        // Now all remaining threads should hit cache
        const int threadCount = 16;
        var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (var t = 0; t < threadCount; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                var result = _service.ReadInFile(0, PageSize);
                CollectionAssert.AreEqual(data, result);
            });
        }

        Task.WaitAll(tasks);

        // No additional misses — all concurrent reads hit the pre-warmed cache
        Assert.AreEqual(1, _service.CacheMissCount,
            "No additional cache miss expected after pre-warming");
        Assert.AreEqual(threadCount, _service.CacheHitCount,
            $"Expected {threadCount} cache hits after pre-warming");
    }

    [TestMethod]
    public void TestConcurrentCacheEvictionUnderHighContention()
    {
        // Use a small cache (4 pages) so evictions happen frequently under contention.
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }
        _service = new CachedFileAccessService(TestFilePath,
            initialUnderlyingFileSizeIfNotExists: InitialSize,
            cachePageSize: PageSize,
            maxCachedPagesCount: 4,
            hotCacheItems: 0);

        const int pageCount = 32;
        var expected = new byte[pageCount][];
        for (var i = 0; i < pageCount; i++)
        {
            expected[i] = new byte[PageSize];
            new Random(i).NextBytes(expected[i]);
            _service.WriteInFile(i * (long)PageSize, expected[i]);
        }

        _service.ResetAllStatistics();

        const int threadCount = 16;
        var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (var t = 0; t < threadCount; t++)
        {
            var threadId = t;
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                // Each thread reads all pages in random order to maximize eviction pressure
                var rng = new Random(threadId);
                var indices = Enumerable.Range(0, pageCount).OrderBy(_ => rng.Next()).ToArray();
                foreach (var pageIndex in indices)
                {
                    var result = _service.ReadInFile(pageIndex * (long)PageSize, PageSize);
                    CollectionAssert.AreEqual(
                        expected[pageIndex], result,
                        $"Data mismatch on page {pageIndex} from thread {threadId}");
                }
            });
        }

        Task.WaitAll(tasks);

        // Verify evictions occurred (small cache with many pages guarantees this)
        Assert.IsTrue(_service.RemoveFromCacheCount > 0,
            "Expected cache evictions under high contention with a small cache");
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
