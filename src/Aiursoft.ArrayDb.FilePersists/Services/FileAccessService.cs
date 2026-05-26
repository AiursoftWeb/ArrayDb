using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Aiursoft.ArrayDb.FilePersists.Services;

public class FileAccessService : IDisposable
{
    public readonly string Path;
    public int SeekWriteCount;
    public int SeekReadCount;
    public int ExpandSizeCount;
    private long _currentSize;
    private readonly object _expandSizeLock = new();
    private readonly long _initialSizeIfNotExists;
    private SafeFileHandle _fileHandle;
    private bool _disposed;

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
            FillFileStream(fs, 0, initialSizeIfNotExists);
        }

        _currentSize = new FileInfo(path).Length;
        _fileHandle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, GetFileShare(), FileOptions.RandomAccess);
    }

    public void WriteInFile(long offset, byte[] data)
    {
        ExpandFileIfNeededThreadSafe(offset, data.Length);
        RandomAccess.Write(_fileHandle, data, offset);
        Interlocked.Increment(ref SeekWriteCount);
    }

    public void WriteInFile(long offset, ReadOnlySpan<byte> data)
    {
        ExpandFileIfNeededThreadSafe(offset, data.Length);
        RandomAccess.Write(_fileHandle, data, offset);
        Interlocked.Increment(ref SeekWriteCount);
    }

    public byte[] ReadInFile(long offset, int length)
    {
        ExpandFileIfNeededThreadSafe(offset, length);
        var buffer = new byte[length];
        var read = RandomAccess.Read(_fileHandle, buffer, offset);
        Interlocked.Increment(ref SeekReadCount);
        if (read != length)
        {
            throw new Exception("Failed to read the expected length of data");
        }

        return buffer;
    }

    public async Task DeleteAsync()
    {
        _fileHandle.Close();
        await Task.Run(() =>
        {
            lock (_expandSizeLock)
            {
                File.Delete(Path);
            }
        });
    }

    public void Dispose()
    {
        if (_disposed) return;
        _fileHandle.Close();
        _disposed = true;
    }

    private void ExpandFileIfNeededThreadSafe(long offset, int dataLength)
    {
        // For most cases, we don't need to expand the file
        // Make it true statement in if to make CPU branch prediction faster
        if (offset + dataLength <= _currentSize) return;

        lock (_expandSizeLock)
        {
            var sizeToAdjust = _currentSize;
            if (offset + dataLength > sizeToAdjust)
            {
                while (offset + dataLength > sizeToAdjust)
                {
                    sizeToAdjust *= 2;
                }

                using (var fs = new FileStream(Path, FileMode.Open, FileAccess.Write, GetFileShare()))
                {
                    fs.SetLength(sizeToAdjust);
                }

                FillFile(_fileHandle, sizeToAdjust / 2, sizeToAdjust);
                _currentSize = sizeToAdjust;
                Interlocked.Increment(ref ExpandSizeCount);
            }
        }
    }


    /// <summary>
    /// Fill the file with 0 to make file system allocate the sequential space
    ///
    /// This method is not thread-safe. It should be called within a lock.
    /// </summary>
    /// <param name="handle">The safe file handle to write to</param>
    /// <param name="start">The start position to fill</param>
    /// <param name="end">The end position to fill</param>
    private void FillFile(SafeFileHandle handle, long start, long end)
    {
        long currentOffset = start;
        var buffer = new byte[_initialSizeIfNotExists];
        while (currentOffset < end)
        {
            RandomAccess.Write(handle, buffer, currentOffset);
            currentOffset += buffer.Length;
        }
    }

    private void FillFileStream(FileStream fs, long start, long end)
    {
        fs.Seek(start, SeekOrigin.Begin);
        var buffer = new byte[_initialSizeIfNotExists];
        while (fs.Position < end)
        {
            fs.Write(buffer, 0, buffer.Length);
        }
    }

    private static FileShare GetFileShare()
    {
        // If Windows, return FileShare.ReadWrite. If Linux, return FileShare.Read
        return RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? FileShare.ReadWrite : FileShare.Read;
    }
}
