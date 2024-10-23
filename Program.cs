using System.Runtime.CompilerServices;
using System.Text;

File.Delete("sampleData.bin");
File.Delete("sampleDataStrings.bin");
var persistService = new CollectionOnDisk<SampleData>("sampleData.bin", "sampleDataStrings.bin");

for (var i = 0; i < 2; i++)
{
    var sample = new SampleData
    {
        MyNumber1 = i,
        MyString1 = $"Hello, World! 你好世界 {i}",
        MyNumber2 = i * 10,
        MyBoolean1 = i % 2 == 0,
        MyString2 = $"This is another longer string. {i}"
    };
    await persistService.AddAsync(sample);
}

for (var i = 0; i < 2; i++)
{
    var readSample = await persistService.ReadAsync(i);
    Console.WriteLine(readSample.MyString1);
    Console.WriteLine(readSample.MyString2);
    Console.WriteLine(readSample.MyNumber1);
    Console.WriteLine(readSample.MyNumber2);
    Console.WriteLine(readSample.MyBoolean1);
    Console.WriteLine(readSample.MyDateTime);
}

Console.WriteLine("Done. Length now is " + persistService.Length);

public class SampleData
{
    public int MyNumber1 { get; init; }
    public string MyString1 { get; init; } = string.Empty;
    public int MyNumber2 { get; init; }
    public bool MyBoolean1 { get; init; }
    public string MyString2 { get; init; } = string.Empty;
    public DateTime MyDateTime { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// CollectionOnDisk is a generic class that provides methods
/// for persistent storage of objects to disk with functionalities
/// for adding, reading, and writing objects by index.
///
/// This class is more safe than the ObjectPersistOnDiskService class. Because it will avoid reading the uninitialized data.
/// </summary>
/// <typeparam name="T">The type of object to persist on disk, which must have a parameterless constructor.</typeparam>
public class CollectionOnDisk<T>(string structureFilePath, string stringFilePath, long initialSizeIfNotExists = 0x10000)
    where T : new()
{
    public int Length { get; private set; }
    private readonly ObjectPersistOnDiskService<T> _persistService = new(structureFilePath, stringFilePath, initialSizeIfNotExists);
    public async Task AddAsync(T obj)
    {
        var indexToWrite = Length;
        await _persistService.WriteIndexAsync(indexToWrite, obj);
        Length++;
    }

    public Task<T> ReadAsync(int index)
    {
        if (index >= Length)
        {
            throw new IndexOutOfRangeException();
        }
        return _persistService.ReadAsync(index);
    }
}

/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <param name="structureFilePath">The path to the file that stores the structure of the objects.</param>
/// <param name="stringFilePath">The path to the file that stores the string data.</param>
/// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
/// <typeparam name="T"></typeparam>
public class ObjectPersistOnDiskService<T>(string structureFilePath, string stringFilePath, long initialSizeIfNotExists)
    where T : new()
{
    private readonly FileAccessService _structureFileAccess = new(structureFilePath, initialSizeIfNotExists);
    private readonly StringRepository _stringRepository = new(stringFilePath, initialSizeIfNotExists);
   
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

    private async Task SerializeBytesAsync(T obj, byte[] buffer, int offset)
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
                var (stringOffset, stringLength) = await _stringRepository.WriteStringContentAndGetOffset(stringValue);
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

    private async Task<T> DeserializeAsync(byte[] buffer)
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
                var stringValue = await _stringRepository.LoadStringContentAsync(stringOffset, stringLength);
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

    public async Task WriteIndexAsync(long index, T obj)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject];
        await SerializeBytesAsync(obj, buffer, 0);
        await _structureFileAccess.WriteInFile(sizeOfObject * index, buffer);
    }
    
    public async Task<T> ReadAsync(long index)
    {
        var sizeOfObject = GetItemSize();
        var data = await _structureFileAccess.ReadInFile(sizeOfObject * index, sizeOfObject);
        return await DeserializeAsync(data);
    }
    
    [Obsolete(error: false, message: "This method is not intended to be used. Please use ReadAsync or WriteAsync instead.")]
    public T this[long index]
    {
        get => ReadAsync(index).Result;
        set => WriteIndexAsync(index, value).Wait();
    }
}

/// <summary>
/// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
/// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
/// </summary>
public class StringRepository(string stringFilePath, long initialSizeIfNotExists)
{
    private readonly FileAccessService _fileAccess = new(stringFilePath, initialSizeIfNotExists);

    private const int EndOffsetSize = sizeof(long); // We reserve the first 8 bytes for EndOffset

    private async Task<long> GetStringFileEndOffsetAsync()
    {
        var buffer = await _fileAccess.ReadInFile(0, EndOffsetSize);
        return BitConverter.ToInt64(buffer, 0);
    }

    private async Task UpdateStringFileEndOffset(long newOffset)
    {
        var buffer = BitConverter.GetBytes(newOffset);
        await _fileAccess.WriteInFile(0, buffer);
    }
    
    public async Task<(long offset, int stringLength)> WriteStringContentAndGetOffset(string str)
    {
        if (string.IsNullOrEmpty(str))
        {
            return (-1, 0); // -1 offset indicates empty string
        }

        var stringBytes = Encoding.UTF8.GetBytes(str);
        var currentOffset = await GetStringFileEndOffsetAsync();
        
        // Adjust offset to account for reserved EndOffset space
        var newOffset = currentOffset + EndOffsetSize;

        // Save the string content to the string file
        await _fileAccess.WriteInFile(newOffset, stringBytes);

        // Update the end offset to include the length of the new string
        await UpdateStringFileEndOffset(currentOffset + stringBytes.Length);
        
        return (newOffset, stringBytes.Length);
    }

    public async Task<string> LoadStringContentAsync(long offset, int length)
    {
        if (offset == -1)
        {
            return string.Empty;
        }

        // Adjust offset for reserved EndOffset space
        var adjustedOffset = offset + EndOffsetSize;

        var stringBytes = await _fileAccess.ReadInFile(adjustedOffset, length);
        return Encoding.UTF8.GetString(stringBytes);
    }
}

/// <summary>
/// FileAccessService provides methods to perform asynchronous read and write operations on a file.
/// </summary>
public class FileAccessService
{
    private readonly string _path;
    private long _currentSize;
    private readonly object _expandSizeLock = new();

    /// <summary>
    /// FileAccessService provides methods to perform asynchronous read and write operations on a file.
    /// </summary>
    /// <param name="path">The path to the file.</param>
    /// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
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

    public async Task WriteInFile(long offset, byte[] data)
    {
        lock (_expandSizeLock)
        {
            while (offset + data.Length > _currentSize)
            {
                Console.WriteLine($"For file {_path}, After writing content with length {data.Length}, file will be expanded to {_currentSize + data.Length}. However, currently file is {_currentSize}. We will double the size of the file to {_currentSize * 2}");
                using var fs = new FileStream(_path, FileMode.Open, FileAccess.Write);
                fs.SetLength(_currentSize * 2);
                _currentSize = fs.Length;
            }
        }

        await using (var fs = new FileStream(_path, FileMode.Open, FileAccess.Write))
        {
            fs.Seek(offset, SeekOrigin.Begin);
            await fs.WriteAsync(data);
        }
    }

    public async Task<byte[]> ReadInFile(long offset, int length)
    {
        if (offset + length > _currentSize)
        {
            throw new Exception("Exceeded the file size while reading");
        }

        await using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read);
        fs.Seek(offset, SeekOrigin.Begin);
        var buffer = new byte[length];
        var readAsync = await fs.ReadAsync(buffer.AsMemory(0, length));
        if (readAsync != length)
        {
            throw new Exception("Failed to read the expected length of data");
        }

        return buffer;
    }
}
