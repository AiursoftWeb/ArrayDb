using System.Runtime.CompilerServices;
using System.Text;

namespace Aiursoft.ArrayDb;

/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ObjectPersistOnDiskService<T> where T : new()
{
    public int Length;
    private const int LengthMarkerSize = sizeof(int); // We reserve the first 4 bytes for Length
    private readonly FileAccessService _structureFileAccess;
    private readonly StringRepository _stringRepository;

    /// <summary>
    /// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
    /// </summary>
    /// <param name="structureFilePath">The path to the file that stores the structure of the objects.</param>
    /// <param name="stringFilePath">The path to the file that stores the string data.</param>
    /// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
    /// <typeparam name="T"></typeparam>
    public ObjectPersistOnDiskService(string structureFilePath, string stringFilePath, long initialSizeIfNotExists)
    {
        _structureFileAccess = new(structureFilePath, initialSizeIfNotExists);
        _stringRepository = new(stringFilePath, initialSizeIfNotExists);
        Length = GetLength();
    }
    
    private int GetLength()
    {
        var buffer = _structureFileAccess.ReadInFile(0, LengthMarkerSize);
        return BitConverter.ToInt32(buffer, 0);
    }
    
    private void SetLength(int length)
    {
        var buffer = BitConverter.GetBytes(length);
        _structureFileAccess.WriteInFile(0, buffer);
    }

    public int GetItemSize()
    {
        var size = 0;
        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(bool))
            {
                size += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                size += Unsafe.SizeOf<long>() * 2; // Offset and Length (2 long values)
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                size += Unsafe.SizeOf<long>(); // Size of DateTime (stored as Ticks, which is a long)
            }
            else
            {
                throw new Exception($"Unsupported property type: {prop.PropertyType}");
            }
        }
        return size;
    }

    private void SerializeBytes(T obj, byte[] buffer, int offset)
    {
        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(bool))
            {
                // Bool should be stored as 1 or 0
                var value = (bool)prop.GetValue(obj)! ? 1 : 0;
                Unsafe.WriteUnaligned(ref buffer[offset], value);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(int))
            {
                // Int should be stored as int
                var value = prop.GetValue(obj);
                Unsafe.WriteUnaligned(ref buffer[offset], (int)value!);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                // String should be stored as Offset (long) and Length (int)
                var stringValue = (string)prop.GetValue(obj)!;
                var (stringOffset, stringLength) = _stringRepository.WriteStringContentAndGetOffset(stringValue);
                Unsafe.WriteUnaligned(ref buffer[offset], stringOffset);
                offset += Unsafe.SizeOf<long>();
                Unsafe.WriteUnaligned(ref buffer[offset], stringLength);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                // DateTime should be stored as Ticks (long)
                var dateTimeValue = (DateTime)prop.GetValue(obj)!;
                var ticks = dateTimeValue.Ticks;
                Unsafe.WriteUnaligned(ref buffer[offset], ticks);
                offset += Unsafe.SizeOf<long>();
            }
            else
            {
                throw new Exception($"Unsupported property type: {prop.PropertyType}");
            }
        }
    }

    private T Deserialize(byte[] buffer)
    {
        var obj = new T();
        var offset = 0;

        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(bool))
            {
                var value = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                if (prop.PropertyType == typeof(bool))
                {
                    prop.SetValue(obj, value != 0);
                }
                else
                {
                    prop.SetValue(obj, value);
                }
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                // Read the Offset and Length
                var stringOffset = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                var stringLength = Unsafe.ReadUnaligned<int>(ref buffer[offset + Unsafe.SizeOf<long>()]);

                // Read the string from the string file
                var stringValue = _stringRepository.LoadStringContent(stringOffset, stringLength);
                prop.SetValue(obj, stringValue);

                offset += Unsafe.SizeOf<long>() + Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                var ticks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                var dateTimeValue = new DateTime(ticks);
                prop.SetValue(obj, dateTimeValue);
                offset += Unsafe.SizeOf<long>();
            }
        }
        return obj;
    }

    public void WriteIndex(long index, T obj)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject];
        SerializeBytes(obj, buffer, 0);
        _structureFileAccess.WriteInFile(sizeOfObject * index + LengthMarkerSize, buffer);
    }
    
    public void WriteBulk(long index, T[] objs)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject * objs.Length];
        for (var i = 0; i < objs.Length; i++)
        {
            SerializeBytes(objs[i], buffer, sizeOfObject * i);
        }
        _structureFileAccess.WriteInFile(sizeOfObject * index + LengthMarkerSize, buffer);
    }
    
    public void Add(T obj)
    {
        var indexToWrite = Length;
        WriteIndex(indexToWrite, obj);
        Length++;
        SetLength(Length);
    }
    
    public void AddBulk(T[] objs)
    {
        var indexToWrite = Length;
        WriteBulk(indexToWrite, objs);
        Length += objs.Length;
        SetLength(Length);
    }
    
    public T Read(long index)
    {
        var sizeOfObject = GetItemSize();
        var data = _structureFileAccess.ReadInFile(sizeOfObject * index + LengthMarkerSize, sizeOfObject);
        return Deserialize(data);
    }
    
    public T this[long index]
    {
        get => Read(index);
        set => WriteIndex(index, value);
    }
}

/// <summary>
/// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
/// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
/// </summary>
public class StringRepository
{
    private readonly FileAccessService _fileAccess;
    public long FileEndOffset;
    private const int EndOffsetSize = sizeof(long); // We reserve the first 8 bytes for EndOffset

    /// <summary>
    /// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
    /// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
    /// </summary>
    public StringRepository(string stringFilePath, long initialSizeIfNotExists)
    {
        _fileAccess = new(stringFilePath, initialSizeIfNotExists);
        FileEndOffset = GetStringFileEndOffset();
    }

    private long GetStringFileEndOffset()
    {
        var buffer = _fileAccess.ReadInFile(0, EndOffsetSize);
        var offSet = BitConverter.ToInt64(buffer, 0);
        // When initially the file is empty, we need to reserve the first 8 bytes for EndOffset
        return offSet <= EndOffsetSize ? EndOffsetSize : offSet;
    }

    public (long offset, int stringLength) WriteStringContentAndGetOffset(string? str)
    {
        switch (str)
        {
            case "":
                return (-1, 0); // -1 offset indicates empty string
            case null:
                return (-2, 0); // -2 offset indicates null string
        }

        var stringBytes = Encoding.UTF8.GetBytes(str);
        var currentOffset = FileEndOffset;
        // Save the string content to the string file
        _fileAccess.WriteInFile(currentOffset, stringBytes);
        
        // Update the end offset in the string file
        var newOffset = currentOffset + stringBytes.Length;
        var buffer = BitConverter.GetBytes(newOffset);
        _fileAccess.WriteInFile(0, buffer);
        
        // Update the end offset in memory
        FileEndOffset = newOffset;
        
        return (currentOffset, stringBytes.Length);
    }

    public string? LoadStringContent(long offset, int length)
    {
        switch (offset)
        {
            case -1:
                return string.Empty;
            case -2:
                return null;
            default:
            {
                var stringBytes = _fileAccess.ReadInFile(offset, length);
                return Encoding.UTF8.GetString(stringBytes);
            }
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
                using var fs = new FileStream(_path, FileMode.Open, FileAccess.Write);
                fs.SetLength(_currentSize * 2);
                _currentSize = fs.Length;
            }
        }

        using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            fs.Write(data);
        }
    }

    public byte[] ReadInFile(long offset, int length)
    {
        if (offset + length > _currentSize)
        {
            throw new Exception("Exceeded the file size while reading");
        }
        
        using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read);
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