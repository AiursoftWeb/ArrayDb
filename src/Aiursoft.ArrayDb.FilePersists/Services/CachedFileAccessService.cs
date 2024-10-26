using System.Diagnostics.CodeAnalysis;

namespace Aiursoft.ArrayDb.FilePersists.Services;

/// <summary>
/// Represents a service for accessing and caching file data.
/// </summary>
public class CachedFileAccessService(
    string path,
    long initialUnderlyingFileSizeIfNotExists, // 16 MB
    int cachePageSize, // 16 MB
    int maxCachedPagesCount, // 64 pages cached in memory at most (1 GB)
    int hotCacheItems) // most recent 16 pages are considered hot and will not be moved even they are used
{
    private readonly FileAccessService _underlyingAccessService = new(path, initialUnderlyingFileSizeIfNotExists);
    private readonly Dictionary<long, byte[]> _cache = new();
    private readonly LinkedList<long> _lruList = new();
    private readonly object _cacheLock = new();
    public int CacheHitCount;
    public int CacheMissCount;
    public int CacheWipeCount;
    public int LruUpdateCount;
    public int RemoveFromCacheCount;
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        CacheHitCount = 0;
        CacheMissCount = 0;
        CacheWipeCount = 0;
        LruUpdateCount = 0;
        RemoveFromCacheCount = 0;
    }

    public string OutputCacheReport()
    {
        return $@"
Cache usage report:

* Cache hit count: {CacheHitCount}
* Cache miss count: {CacheMissCount}
* Cache wipe count: {CacheWipeCount}
* Page move last events count: {LruUpdateCount}
* Remove from cache events count: {RemoveFromCacheCount}

Underlying file access service statistics:
{_underlyingAccessService.OutputStatistics().AppendTabsEachLineHead()}
";
    }

    public void WriteInFile(long offset, byte[] data)
    {
        // Clear cache for the pages that will be overwritten
        lock (_cacheLock)
        {
            var startPage = offset / cachePageSize;
            var endPage = (offset + data.Length) / cachePageSize;
            for (var page = startPage; page <= endPage; page++)
            {
                if (_cache.ContainsKey(page))
                {
                    _cache.Remove(page);
                    _lruList.Remove(page);
                    Interlocked.Increment(ref CacheWipeCount);
                }
            }
        }

        _underlyingAccessService.WriteInFile(offset, data);
    }

    public byte[] ReadInFile(long offset, int length)
    {
        var result = new byte[length];
        var currentOffset = offset;
        var resultOffset = 0;

        while (length > 0)
        {
            var pageOffset = currentOffset / cachePageSize;
            var pageStart = (int)(currentOffset % cachePageSize);
            var bytesToRead = Math.Min(length, cachePageSize - pageStart);

            var pageData = GetPageFromCache(pageOffset);
            Array.Copy(pageData, pageStart, result, resultOffset, bytesToRead);

            currentOffset += bytesToRead;
            resultOffset += bytesToRead;
            length -= bytesToRead;
        }

        return result;
    }

    private byte[] GetPageFromCache(long pageOffset)
    {
        lock (_cacheLock)
        {
            if (_cache.TryGetValue(pageOffset, out var cache))
            {
                if (ShouldUpdateLru(pageOffset))
                {
                    _lruList.Remove(pageOffset);
                    _lruList.AddLast(pageOffset);
                    Interlocked.Increment(ref LruUpdateCount);
                }
                Interlocked.Increment(ref CacheHitCount);
                return cache;
            }

            var pageData = _underlyingAccessService.ReadInFile(pageOffset * cachePageSize, cachePageSize);
            AddToCache(pageOffset, pageData);
            Interlocked.Increment(ref CacheMissCount);
            return pageData;
        }
    }

    private bool ShouldUpdateLru(long pageOffset)
    {
        var pointer = _lruList.Last;
        if (pointer == null)
        {
            // No item in LRU list
            return false;
        }
        for (var i = 0; i < hotCacheItems; i++)
        {
            if (pointer!.Value == pageOffset)
            {
                return false;
            }

            pointer = pointer.Previous;
        }

        return true;
    }

    private void AddToCache(long pageOffset, byte[] data)
    {
        lock (_cacheLock)
        {
            while (_cache.Count >= maxCachedPagesCount && _lruList.Count > 0)
            {
                var oldestPage = _lruList.First!.Value;
                _lruList.RemoveFirst();
                _cache.Remove(oldestPage);
                Interlocked.Increment(ref RemoveFromCacheCount);
            }
            
            _cache[pageOffset] = data;
            _lruList.AddLast(pageOffset);
        }
    }
}