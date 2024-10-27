using System.Diagnostics.CodeAnalysis;

namespace Aiursoft.ArrayDb.FilePersists.Services;

public class FileAccessService
{
    public readonly string Path;
    public int SeekWriteCount;
    public int SeekReadCount;
    public int ExpandSizeCount;
    private long _currentSize;
    private readonly object _expandSizeLock = new();
    private readonly long _initialSizeIfNotExists;
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        SeekWriteCount = 0;
        SeekReadCount = 0;
        ExpandSizeCount = 0;
    }
    
    public string OutputStatistics()
    {
        lock (_expandSizeLock)
        {
            return $@"
File access service statistics:

* File path: {Path}
* Initial size if not exists (in MB): {_initialSizeIfNotExists / 1024 / 1024}
* Actual Seek write events count: {SeekWriteCount}
* Actual Seek read  events count: {SeekReadCount}
* Expand physical file size events count: {ExpandSizeCount}
* Current physical file size (in MB): {_currentSize / 1024 / 1024}
";
        }
    }
    
    public FileAccessService(string path, long initialSizeIfNotExists)
    {
        Path = path;
        _initialSizeIfNotExists = initialSizeIfNotExists;
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
            if (offset + data.Length > _currentSize)
            {
                while (offset + data.Length > _currentSize)
                {
                    _currentSize *= 2;
                }

                using var fs = new FileStream(Path, FileMode.Open, FileAccess.Write);
                fs.SetLength(_currentSize);
                Interlocked.Increment(ref ExpandSizeCount);
            }
        }

        using (var fs = new FileStream(Path, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            fs.Write(data);
            Interlocked.Increment(ref SeekWriteCount);
        }
    }

    public byte[] ReadInFile(long offset, int length)
    {
        lock (_expandSizeLock)
        {
            if (offset + length > _currentSize)
            {
                while (offset + length > _currentSize)
                {
                    _currentSize *= 2;
                }

                using var fs = new FileStream(Path, FileMode.Open, FileAccess.Write);
                fs.SetLength(_currentSize);
                Interlocked.Increment(ref ExpandSizeCount);
            }
        }

        using (var fs = new FileStream(Path, FileMode.Open, FileAccess.Read))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            var buffer = new byte[length];
            var read = fs.Read(buffer, 0, length);
            Interlocked.Increment(ref SeekReadCount);
            if (read != length)
            {
                throw new Exception("Failed to read the expected length of data");
            }

            return buffer;
        }
    }
}