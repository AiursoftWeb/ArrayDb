namespace Aiursoft.ArrayDb;

public class CachedFileAccessService(
    string path,
    long initialSizeIfNotExists,
    int pageSize = 1024 * 1024, // 1MB
    int maxCacheItems = 512) // 512MB
{
    private readonly FileAccessService _fileAccessService = new(path, initialSizeIfNotExists);
    private readonly Dictionary<long, byte[]> _cache = new();
    private readonly LinkedList<long> _lruList = new();
    private readonly object _cacheLock = new();

    public void WriteInFile(long offset, byte[] data)
    {
        // Clear cache for the pages that will be overwritten
        lock (_cacheLock)
        {
            var startPage = offset / pageSize;
            var endPage = (offset + data.Length) / pageSize;
            for (var page = startPage; page <= endPage; page++)
            {
                if (_cache.ContainsKey(page))
                {
                    _cache.Remove(page);
                    _lruList.Remove(page);
                }
            }
        }

        _fileAccessService.WriteInFile(offset, data);
    }

    public byte[] ReadInFile(long offset, int length)
    {
        // TODO: Bad performance with List. Refactor with byte[] and Array.Copy
        var result = new List<byte>();
        var currentOffset = offset;

        while (length > 0)
        {
            var pageOffset = currentOffset / pageSize;
            var pageStart = (int)(currentOffset % pageSize);
            var bytesToRead = Math.Min(length, pageSize - pageStart);

            var pageData = GetPageFromCache(pageOffset);
            result.AddRange(new ArraySegment<byte>(pageData, pageStart, bytesToRead));

            currentOffset += bytesToRead;
            length -= bytesToRead;
        }

        return result.ToArray();
    }

    private byte[] GetPageFromCache(long pageOffset)
    {
        lock (_cacheLock)
        {
            if (_cache.TryGetValue(pageOffset, out var cache))
            {
                // Cache hit. Update LRU list
                var needUpdateLru = ShouldUpdateLru(pageOffset);
                if (needUpdateLru)
                {
                    _lruList.Remove(pageOffset);
                    _lruList.AddLast(pageOffset);
                }

                return cache;
            }

            // Cache miss. Read from file
            var pageData = _fileAccessService.ReadInFile(pageOffset * pageSize, pageSize);
            AddToCache(pageOffset, pageData);
            return pageData;
        }
    }

    private bool ShouldUpdateLru(long pageOffset)
    {
        const int veryRecentPageAccessLimit = 0x10; // 16

        var pointer = _lruList.Last;
        if (pointer == null)
        {
            // No item in LRU list
            return false;
        }
        for (int i = 0; i < veryRecentPageAccessLimit; i++)
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
            while (_cache.Count >= maxCacheItems && _lruList.Count > 0)
            {
                var oldestPage = _lruList.First!.Value;
                _lruList.RemoveFirst();
                _cache.Remove(oldestPage);
            }
            
            _cache[pageOffset] = data;
            _lruList.AddLast(pageOffset);
        }
    }
}

public class FileAccessService
{
    private readonly string _path;
    private long _currentSize;
    private readonly object _expandSizeLock = new();

    public FileAccessService(string path, long initialSizeIfNotExists)
    {
        _path = path;
        if (!File.Exists(path))
        {
            using var fs = File.Create(path);
            fs.SetLength(initialSizeIfNotExists);
        }

        _currentSize = new FileInfo(path).Length;
    }

    public void WriteInFile(long offset, byte[] data)
    {
        lock (_expandSizeLock)
        {
            while (offset + data.Length > _currentSize)
            {
                _currentSize *= 2;
            }
            using var fs = new FileStream(_path, FileMode.Open, FileAccess.Write);
            fs.SetLength(_currentSize);
        }

        using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            fs.Write(data);
        }
    }

    public byte[] ReadInFile(long offset, int length)
    {
        lock (_expandSizeLock)
        {
            while (offset + length > _currentSize)
            {
                _currentSize *= 2;
            }
            using var fs = new FileStream(_path, FileMode.Open, FileAccess.Write);
            fs.SetLength(_currentSize);
        }

        using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Read))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            var buffer = new byte[length];
            var read = fs.Read(buffer, 0, length);
            if (read != length)
            {
                throw new Exception("Failed to read the expected length of data");
            }

            return buffer;
        }
    }
}