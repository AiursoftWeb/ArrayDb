namespace Aiursoft.ArrayDb;

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