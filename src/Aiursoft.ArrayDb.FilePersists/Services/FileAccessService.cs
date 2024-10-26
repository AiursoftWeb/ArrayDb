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
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        SeekWriteCount = 0;
        SeekReadCount = 0;
        ExpandSizeCount = 0;
    }
    
    public string OutputStatistics()
    {
        return $@"
File access service statistics:

* File path: {Path}
* Actual Seek write events count: {SeekWriteCount}
* Actual Seek read  events count: {SeekReadCount}
* Expand physical file size events count: {ExpandSizeCount}
";
    }
    
    public FileAccessService(string path, long initialSizeIfNotExists)
    {
        Path = path;
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
            using var fs = new FileStream(Path, FileMode.Open, FileAccess.Write);
            fs.SetLength(_currentSize);
            Interlocked.Increment(ref ExpandSizeCount);
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
            while (offset + length > _currentSize)
            {
                _currentSize *= 2;
            }
            using var fs = new FileStream(Path, FileMode.Open, FileAccess.Write);
            fs.SetLength(_currentSize);
            Interlocked.Increment(ref ExpandSizeCount);
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