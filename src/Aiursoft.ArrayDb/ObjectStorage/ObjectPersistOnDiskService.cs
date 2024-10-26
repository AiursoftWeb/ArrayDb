using System.Runtime.CompilerServices;
using System.Text;
using Aiursoft.ArrayDb.FileSystem;
using Aiursoft.ArrayDb.Models;

namespace Aiursoft.ArrayDb.ObjectStorage;

/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ObjectPersistOnDiskService<T> where T : new()
{
    // Save the offset
    public long Count;
    private const int CountMarkerSize = sizeof(long);
    private readonly object _expandLengthLock = new();
    
    // Underlying store
    public readonly CachedFileAccessService StructureFileAccess;
    public readonly StringRepository StringRepository;

    /// <summary>
    /// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
    /// </summary>
    /// <param name="structureFilePath">The path to the file that stores the structure of the objects.</param>
    /// <param name="stringFilePath">The path to the file that stores the string data.</param>
    /// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
    /// <typeparam name="T"></typeparam>
    public ObjectPersistOnDiskService(string structureFilePath, string stringFilePath, long initialSizeIfNotExists)
    {
        StructureFileAccess = new(structureFilePath, initialSizeIfNotExists);
        StringRepository = new(stringFilePath, initialSizeIfNotExists);
        Count = GetItemsCount();
    }
    
    private long GetItemsCount()
    {
        var buffer = StructureFileAccess.ReadInFile(0, CountMarkerSize);
        return BitConverter.ToInt32(buffer, 0);
    }
    
    private void SaveCount(long length)
    {
        var buffer = BitConverter.GetBytes(length);
        StructureFileAccess.WriteInFile(0, buffer);
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
                size += Unsafe.SizeOf<long>(); // Size of Offset (stored as long)
                size += Unsafe.SizeOf<int>(); // Size of Length (stored as int)
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

    // T to OWPST
    private ObjectWithPersistedStrings<T>[] SaveObjectStrings(T[] objs)
    {
        var stringsCount = typeof(T).GetProperties().Count(p => p.PropertyType == typeof(string));
        var properties = typeof(T).GetProperties().Where(p => p.PropertyType == typeof(string)).ToArray();
        var strings = objs
            .SelectMany(obj => properties.Select(p => (string?)p.GetValue(obj)))
            .Select(str =>
            {
                if (str == null) return new ProcessingString { Length = 0, Bytes = [] };
                var stringBytes = Encoding.UTF8.GetBytes(str);
                return new ProcessingString { Length = stringBytes.Length, Bytes = stringBytes };
            })
            .ToArray();

        var savedStrings = StringRepository.BulkWriteStringContentAndGetOffsetV2(strings);
        var result = new ObjectWithPersistedStrings<T>[objs.Length];
        for (int i = 0; i < objs.Length; i++)
        {
            var obj = objs[i];
            result[i] = new ObjectWithPersistedStrings<T>
            {
                Object = obj,
                Strings = savedStrings.Skip(i * stringsCount).Take(stringsCount)
            };
        }
        return result;
    }
    
    // OWPST to bytes.
    private void SerializeBytes(ObjectWithPersistedStrings<T> objWithStrings, byte[] buffer, int offset)
    {
        // Save the object properties
        var innerObject = objWithStrings.Object;
        var properties = typeof(T).GetProperties();
        foreach (var prop in properties)
        {
            if (prop.PropertyType == typeof(bool))
            {
                // Bool should be stored as 1 or 0
                var value = (bool)prop.GetValue(innerObject)! ? 1 : 0;
                Unsafe.WriteUnaligned(ref buffer[offset], value);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(int))
            {
                // Int should be stored as int
                var value = prop.GetValue(innerObject);
                Unsafe.WriteUnaligned(ref buffer[offset], (int)value!);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                // DateTime should be stored as Ticks (long)
                var dateTimeValue = (DateTime)prop.GetValue(innerObject)!;
                var ticks = dateTimeValue.Ticks;
                Unsafe.WriteUnaligned(ref buffer[offset], ticks);
                offset += Unsafe.SizeOf<long>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                // String should be ignored here. It is stored in the string file.
                // Later at the end of the method, we will save the offsets and lengths of the strings.
            }
            else
            {
                throw new Exception($"Unsupported property type: {prop.PropertyType}");
            }
        }
        
        // Save the strings (Actually, the offsets and lengths of the strings)
        foreach (var stringInByteArray in objWithStrings.Strings)
        {
            Unsafe.WriteUnaligned(ref buffer[offset], stringInByteArray.Offset);
            offset += Unsafe.SizeOf<long>();
            Unsafe.WriteUnaligned(ref buffer[offset], stringInByteArray.Length);
            offset += Unsafe.SizeOf<int>();
        }
    }
    
    // Bytes to T
    private T DeserializeBytes(byte[] buffer, int offset = 0)
    {
        // Load the object basic properties.
        var obj = new T();
        var properties = typeof(T).GetProperties();
        foreach (var prop in properties)
        {
            if (prop.PropertyType == typeof(bool))
            {
                var value = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                prop.SetValue(obj, value == 1);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(int))
            {
                var value = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                prop.SetValue(obj, value);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                var ticks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                prop.SetValue(obj, new DateTime(ticks));
                offset += Unsafe.SizeOf<long>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                // String should be ignored here. It is loaded from the string file.
                // Later at the end of the method, we will load the strings.
            }
            else
            {
                throw new Exception($"Unsupported property type: {prop.PropertyType}");
            }
        }
        
        // Load the object strings.
        foreach (var prop in properties.Where(p => p.PropertyType == typeof(string)))
        {
            var offsetInByteArray = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
            offset += Unsafe.SizeOf<long>();
            var length = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
            offset += Unsafe.SizeOf<int>();
            var str = StringRepository.LoadStringContent(offsetInByteArray, length);
            prop.SetValue(obj, str);
        }
        
        return obj;
    }
    
    private void WriteIndex(long index, T obj)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject];
        // Save the strings.
        var objWithStrings = SaveObjectStrings([obj])[0];
        SerializeBytes(objWithStrings, buffer, 0);
        StructureFileAccess.WriteInFile(sizeOfObject * index + CountMarkerSize, buffer);
    }
    
    private void WriteBulk(long index, T[] objs)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject * objs.Length];
        // Save the strings.
        var objWithStrings = SaveObjectStrings(objs);
        
        // Sequential write. Serialize objects in parallel.
        Parallel.For(0, objs.Length, i =>
        {
            SerializeBytes(objWithStrings[i], buffer, sizeOfObject * i);
        });
        
        // Sequential write. Write binary data to disk.
        StructureFileAccess.WriteInFile(sizeOfObject * index + CountMarkerSize, buffer);
    }
    
    [Obsolete(error: false, message: "Write objects one by one is slow. Use AddBulk instead.")]
    public void Add(T obj)
    {
        var indexToWrite = RequestWriteSpaceAndGetStartOffset(1);
        WriteIndex(indexToWrite, obj);
    }
    
    private long RequestWriteSpaceAndGetStartOffset(int itemsCount)
    {
        long writeOffset;
        lock (_expandLengthLock)
        {
            writeOffset = Count;
            Count += itemsCount;
            SaveCount(Count);
        }
        return writeOffset;
    }
    
    public void AddBulk(T[] objs)
    {
        var indexToWrite = RequestWriteSpaceAndGetStartOffset(objs.Length);
        WriteBulk(indexToWrite, objs);
    }
    
    [Obsolete(error: false, message: "Read objects one by one is slow. Use ReadBulk instead.")]
    public T Read(int index)
    {
        if (index < 0 || index >= Count)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }
        var sizeOfObject = GetItemSize();
        var data = StructureFileAccess.ReadInFile(sizeOfObject * index + CountMarkerSize, sizeOfObject);
        return DeserializeBytes(data);
    }

    public T[] ReadBulk(int indexFrom, int count)
    {
        if (indexFrom < 0 || indexFrom + count > Count)
        {
            throw new ArgumentOutOfRangeException(nameof(indexFrom));
        }
        var sizeOfObject = GetItemSize();
        // Sequential read. Load binary data from disk and deserialize them in parallel.
        var data = StructureFileAccess.ReadInFile(sizeOfObject * indexFrom + CountMarkerSize, sizeOfObject * count);
        var result = new T[count];
        Parallel.For(0, count, i =>
        {
            // TO Optimize: We need to preload the string file, because the string file is accessed randomly.
            // Random access to the string file to load the string data.
            result[i] = DeserializeBytes(data, sizeOfObject * i);
        });
        return result;
    }
}

