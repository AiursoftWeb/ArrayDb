using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using Aiursoft.ArrayDb.Engine.Models;
using Aiursoft.ArrayDb.FilePersists;
using Aiursoft.ArrayDb.FilePersists.Services;

namespace Aiursoft.ArrayDb.Engine.ObjectStorage;

/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ObjectRepository<T> where T : new()
{
    // Save the offset
    public long Count;
    private const int CountMarkerSize = sizeof(long);
    private readonly object _expandLengthLock = new();
    
    // Underlying store
    public readonly CachedFileAccessService StructureFileAccess;
    public readonly StringRepository StringRepository;
    
    // Statistics
    public int SingleAppendCount;
    public int BulkAppendCount;
    public int ReadCount;
    public int ReadBulkCount;
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        SingleAppendCount = 0;
        BulkAppendCount = 0;
        ReadCount = 0;
        ReadBulkCount = 0;
    }
    
    public string OutputStatistics()
    {
        return $@"
Object repository with item type {typeof(T).Name} statistics:

* Stored objects count: {Count}
* Single append events count: {SingleAppendCount}
* Bulk   append events count: {BulkAppendCount}
* Read      events count: {ReadCount}
* Read bulk events count: {ReadBulkCount}

Underlying structure file access service statistics:
{StructureFileAccess.OutputCacheReport().AppendTabsEachLineHead()}

Underlying string repository statistics:
{StringRepository.OutputStatistics().AppendTabsEachLineHead()}
";
    }

    /// <summary>
    /// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
    /// </summary>
    /// <param name="structureFilePath">The path to the file that stores the structure of the objects.</param>
    /// <param name="stringFilePath">The path to the file that stores the string data.</param>
    /// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
    /// <param name="cachePageSize">The size of the cache page.</param>
    /// <param name="maxCachedPagesCount">The maximum number of pages cached in memory.</param>
    /// <param name="hotCacheItems">The number of most recent pages that are considered hot and will not be moved even if they are used.</param>
    /// <typeparam name="T"></typeparam>
    public ObjectRepository(
        string structureFilePath, 
        string stringFilePath, 
        long initialSizeIfNotExists = Consts.DefaultFileSize,
        int cachePageSize = Consts.CachePageSize,
        int maxCachedPagesCount = Consts.MaxCachedPagesCount,
        int hotCacheItems = Consts.HotCacheItems)
    {
        StructureFileAccess = new(
            path: structureFilePath,
            initialUnderlyingFileSizeIfNotExists: initialSizeIfNotExists,
            cachePageSize: cachePageSize,
            maxCachedPagesCount: maxCachedPagesCount,
            hotCacheItems: hotCacheItems);
        StringRepository = new(
            stringFilePath: stringFilePath,
            initialUnderlyingFileSizeIfNotExists: initialSizeIfNotExists,
            cachePageSize: cachePageSize,
            maxCachedPagesCount: maxCachedPagesCount,
            hotCacheItems: hotCacheItems);
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

    public int GetItemSize()
    {
        var size = 0;
        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(bool))
            {
                size += sizeof(int);
            }
            else if (prop.PropertyType == typeof(string))
            {
                size += sizeof(long) + sizeof(int); // Size of Offset (stored as long) and Length (stored as int)
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                size += sizeof(long); // DateTime is stored as long
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
        var stringProperties = typeof(T).GetProperties().Where(p => p.PropertyType == typeof(string)).ToArray();

        // Convert the strings to byte[]s
        var strings = new byte[objs.Length * stringsCount][];
        Parallel.For(0, objs.Length, i =>
        {
            var obj = objs[i];
            for (var j = 0; j < stringsCount; j++)
            {
                var str = (string?)stringProperties[j].GetValue(obj);
                if (str == null)
                {
                    strings[i * stringsCount + j] = [];
                }
                else
                {
                    strings[i * stringsCount + j] = Encoding.UTF8.GetBytes(str);
                }
            }
        });

        // Convert the byte[]s to SavedStrings
        var savedStrings = StringRepository.BulkWriteStringContentAndGetOffsets(strings);
        
        // Convert the SavedStrings to ObjectWithPersistedStrings
        var result = new ObjectWithPersistedStrings<T>[objs.Length];
        Parallel.For(0, objs.Length, i =>
        {
            result[i] = new ObjectWithPersistedStrings<T>
            {
                Object = objs[i],
                Strings = savedStrings.Skip(i * stringsCount).Take(stringsCount)
            };
        });
        return result;
    }
    
    // OWPST to bytes.
    private void SerializeBytes(ObjectWithPersistedStrings<T> objWithStrings, byte[] buffer, int offset)
    {
        var properties = typeof(T).GetProperties();
        foreach (var prop in properties)
        {
            if (prop.PropertyType == typeof(bool))
            {
                Unsafe.WriteUnaligned(ref buffer[offset], (bool)prop.GetValue(objWithStrings.Object)! ? 1 : 0);
                offset += sizeof(int);
            }
            else if (prop.PropertyType == typeof(int))
            {
                Unsafe.WriteUnaligned(ref buffer[offset], (int)prop.GetValue(objWithStrings.Object)!);
                offset += sizeof(int);
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                Unsafe.WriteUnaligned(ref buffer[offset], ((DateTime)prop.GetValue(objWithStrings.Object)!).Ticks);
                offset += sizeof(long);
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
        foreach (var str in objWithStrings.Strings)
        {
            Unsafe.WriteUnaligned(ref buffer[offset], str.Offset);
            offset += sizeof(long);
            Unsafe.WriteUnaligned(ref buffer[offset], str.Length);
            offset += sizeof(int);
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
                offset += sizeof(int);
            }
            else if (prop.PropertyType == typeof(int))
            {
                var value = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                prop.SetValue(obj, value);
                offset += sizeof(int);
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                var ticks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                prop.SetValue(obj, new DateTime(ticks));
                offset += sizeof(long);
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
            offset += sizeof(long);
            var length = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
            offset += sizeof(int);
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
        Interlocked.Increment(ref SingleAppendCount);
        WriteIndex(indexToWrite, obj);
    }

    /// <summary>
    /// Add objects in bulk.
    ///
    /// This method is thread-safe. You can call it from multiple threads.
    /// </summary>
    /// <param name="objs">Array of objects to add in bulk.</param>
    public void AddBulk(T[] objs)
    {
        var indexToWrite = RequestWriteSpaceAndGetStartOffset(objs.Length);
        WriteBulk(indexToWrite, objs);
        Interlocked.Increment(ref BulkAppendCount);
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
        Interlocked.Increment(ref ReadCount);
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
            // TODO: Optimize: We need to preload the string file, because the string file is accessed randomly.
            // Random access to the string file to load the string data.
            result[i] = DeserializeBytes(data, sizeOfObject * i);
        });
        Interlocked.Increment(ref ReadBulkCount);
        return result;
    }
}

