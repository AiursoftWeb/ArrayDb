using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.ObjectBucket.Attributes;
using Aiursoft.ArrayDb.ReadLruCache;
using Aiursoft.ArrayDb.StringRepository.Models;

namespace Aiursoft.ArrayDb.ObjectBucket;

// Strong typed.
/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ObjectBucket<T> : IObjectBucket<T> where T : BucketEntity, new()
{
    public int Count => ArchivedItemsCount;
    /// <summary>
    /// SpaceProvisionedItemsCount is the total number of items that the bucket can store.
    ///
    /// When requesting to write new item, it will start to write at the offset of SpaceProvisionedItemsCount.
    /// 
    /// SpaceProvisionedItemsCount is always larger than or equal to ArchivedItemsCount.
    /// </summary>
    public int SpaceProvisionedItemsCount { get; private set; }
    
    /// <summary>
    /// ArchivedItemsCount is the total number of items that have been written to the bucket. Which are the items ready to be read.
    ///
    /// When finished writing new item, the ArchivedItemsCount will be increased by 1.
    ///
    /// ArchivedItemsCount is always less than or equal to SpaceProvisionedItemsCount.
    /// </summary>
    public int ArchivedItemsCount { get; private set; }
    
    private const int CountMarkerSize = sizeof(int) + sizeof(int);
    private readonly object _expandLengthLock = new();

    // Underlying store
    public readonly CachedFileAccessService StructureFileAccess;
    public readonly StringRepository.ObjectStorage.StringRepository StringRepository;

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
        long spaceProvisionedItemsCount;
        lock (_expandLengthLock)
        {
            spaceProvisionedItemsCount = SpaceProvisionedItemsCount;
        }
        // ReSharper disable once InconsistentlySynchronizedField
        return $@"
Object repository with item type {typeof(T).Name} statistics:

* Space provisioned items count: {spaceProvisionedItemsCount}
* Archived items count: {ArchivedItemsCount}
* Consumed actual storage space: {GetItemSize() * spaceProvisionedItemsCount} bytes
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

    public Task SyncAsync()
    {
        // You don't need to sync the string repository, because the string repository is always in sync with the structure file.
        return Task.CompletedTask;
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
    public ObjectBucket(
        string structureFilePath,
        string stringFilePath,
        long initialSizeIfNotExists = Consts.Consts.DefaultPhysicalFileSize,
        int cachePageSize = Consts.Consts.ReadCachePageSize,
        int maxCachedPagesCount = Consts.Consts.MaxReadCachedPagesCount,
        int hotCacheItems = Consts.Consts.ReadCacheHotCacheItems)
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
        (SpaceProvisionedItemsCount,ArchivedItemsCount) = GetItemsCount();
        if (SpaceProvisionedItemsCount != ArchivedItemsCount)
        {
            throw new Exception("The space provisioned items count and archived items count are not equal. The file may be corrupted. Is the process crashed in the middle of writing?");
        }
    }

    private (int provisioned, int archived) GetItemsCount()
    {
        var provisionedAndArchived = StructureFileAccess.ReadInFile(0, CountMarkerSize);
        var provisioned = BitConverter.ToInt32(provisionedAndArchived, 0);
        var archived = BitConverter.ToInt32(provisionedAndArchived, sizeof(int));
        return (provisioned, archived);
    }

    private void SaveCount(int provisioned, int archived)
    {
        var provisionedBytes = BitConverter.GetBytes(provisioned);
        var archivedBytes = BitConverter.GetBytes(archived);
        var buffer = new byte[CountMarkerSize];
        provisionedBytes.CopyTo(buffer, 0);
        archivedBytes.CopyTo(buffer, sizeof(int));
        StructureFileAccess.WriteInFile(0, buffer);
    }

    private long ProvisionWriteSpaceAndGetStartOffset(int itemsCount)
    {
        long writeOffset;
        lock (_expandLengthLock)
        {
            writeOffset = SpaceProvisionedItemsCount;
            SpaceProvisionedItemsCount += itemsCount;
            SaveCount(SpaceProvisionedItemsCount, ArchivedItemsCount);
        }

        return writeOffset;
    }
    
    private void SetArchivedAsProvisioned()
    {
        lock (_expandLengthLock)
        {
            ArchivedItemsCount = SpaceProvisionedItemsCount;
            SaveCount(SpaceProvisionedItemsCount, ArchivedItemsCount);
        }
    }

    private int GetItemSize()
    {
        var size = 0;
        foreach (var prop in typeof(T).GetPropertiesShouldPersistOnDisk())
        {
            switch (Type.GetTypeCode(prop.PropertyType))
            {
                case TypeCode.Int32:
                    size += sizeof(int);
                    break;
                case TypeCode.Boolean:
                    size += sizeof(bool);
                    break;
                case TypeCode.String:
                    size += sizeof(long) + sizeof(int); // Offset as long and Length as int.
                    break;
                case TypeCode.DateTime:
                    size += sizeof(long); // DateTime.Ticks is stored as long.
                    break;
                case TypeCode.Int64:
                    size += sizeof(long);
                    break;
                case TypeCode.Single:
                    size += sizeof(float);
                    break;
                case TypeCode.Double:
                    size += sizeof(double);
                    break;
                default:
                    if (prop.PropertyType == typeof(TimeSpan))
                    {
                        size += sizeof(long); // TimeSpan.Ticks is stored as long.
                    }
                    else if (prop.PropertyType == typeof(Guid))
                    {
                        size += 16; // Guid occupies 16 bytes.
                    }
                    else if (prop.PropertyType == typeof(byte[]))
                    {
                        var lengthAttribute = prop.GetCustomAttribute<FixedLengthStringAttribute>();
                        if (lengthAttribute == null)
                        {
                            throw new Exception("The byte[] property must have a FixedLengthStringAttribute.");
                        }
                        var length = lengthAttribute.BytesLength;
                        size += length;
                    }
                    else
                    {
                        throw new Exception($"Unsupported property type: {prop.PropertyType}");
                    }

                    break;
            }
        }

        return size;
    }

    // T to OWPST
    private ObjectWithPersistedStrings<T>[] SaveObjectStrings(T[] objs)
    {
        var stringsCount = typeof(T).GetPropertiesShouldPersistOnDisk().Count(p => p.PropertyType == typeof(string));
        var stringProperties = typeof(T).GetPropertiesShouldPersistOnDisk().Where(p => p.PropertyType == typeof(string)).ToArray();

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
        objWithStrings.Object.CreationTime = DateTime.UtcNow;
        foreach (var prop in typeof(T).GetPropertiesShouldPersistOnDisk())
        {
            switch (Type.GetTypeCode(prop.PropertyType))
            {
                case TypeCode.Int32:
                    Unsafe.WriteUnaligned(ref buffer[offset], (int)prop.GetValue(objWithStrings.Object)!);
                    offset += sizeof(int);
                    break;

                case TypeCode.Boolean:
                    Unsafe.WriteUnaligned(ref buffer[offset], (bool)prop.GetValue(objWithStrings.Object)! ? 1 : 0);
                    offset += sizeof(bool);
                    break;

                case TypeCode.String:
                    // String should be ignored here. It is stored in the string file.
                    // Offsets and lengths of the strings will be saved at the end of the method.
                    break;

                case TypeCode.DateTime:
                    Unsafe.WriteUnaligned(ref buffer[offset], ((DateTime)prop.GetValue(objWithStrings.Object)!).Ticks);
                    offset += sizeof(long);
                    break;

                case TypeCode.Int64:
                    Unsafe.WriteUnaligned(ref buffer[offset], (long)prop.GetValue(objWithStrings.Object)!);
                    offset += sizeof(long);
                    break;

                case TypeCode.Single:
                    Unsafe.WriteUnaligned(ref buffer[offset], (float)prop.GetValue(objWithStrings.Object)!);
                    offset += sizeof(float);
                    break;

                case TypeCode.Double:
                    Unsafe.WriteUnaligned(ref buffer[offset], (double)prop.GetValue(objWithStrings.Object)!);
                    offset += sizeof(double);
                    break;

                default:
                    if (prop.PropertyType == typeof(TimeSpan))
                    {
                        Unsafe.WriteUnaligned(ref buffer[offset],
                            ((TimeSpan)prop.GetValue(objWithStrings.Object)!).Ticks);
                        offset += sizeof(long);
                    }
                    else if (prop.PropertyType == typeof(Guid))
                    {
                        var guidBytes = ((Guid)prop.GetValue(objWithStrings.Object)!).ToByteArray();
                        for (var i = 0; i < 16; i++)
                        {
                            buffer[offset + i] = guidBytes[i];
                        }

                        offset += 16;
                    }
                    else if (prop.PropertyType == typeof(byte[]))
                    {
                        var lengthAttribute = prop.GetCustomAttribute<FixedLengthStringAttribute>();
                        if (lengthAttribute == null)
                        {
                            throw new Exception("The byte[] property must have a FixedLengthStringAttribute.");
                        }

                        var bytes = (byte[])prop.GetValue(objWithStrings.Object)!;
                        if (bytes.Length > lengthAttribute.BytesLength)
                        {
                            throw new Exception("The byte[] property is too long.");
                        }
                        
                        for (var i = 0; i < bytes.Length; i++)
                        {
                            buffer[offset + i] = bytes[i];
                        }
                        offset += lengthAttribute.BytesLength;
                    }
                    else
                    {
                        throw new Exception($"Unsupported property type: {prop.PropertyType}");
                    }

                    break;
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
        var properties = typeof(T).GetPropertiesShouldPersistOnDisk();
        foreach (var prop in properties)
        {
            switch (Type.GetTypeCode(prop.PropertyType))
            {
                case TypeCode.Int32:
                    var intValue = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                    prop.SetValue(obj, intValue);
                    offset += sizeof(int);
                    break;

                case TypeCode.Boolean:
                    var boolValue = Unsafe.ReadUnaligned<bool>(ref buffer[offset]);
                    prop.SetValue(obj, boolValue);
                    offset += sizeof(bool);
                    break;

                case TypeCode.String:
                    // String should be ignored here. It is loaded from the string file.
                    // Strings will be loaded at the end of the method.
                    break;

                case TypeCode.DateTime:
                    var dateTimeTicks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    prop.SetValue(obj, new DateTime(dateTimeTicks));
                    offset += sizeof(long);
                    break;

                case TypeCode.Int64:
                    var longValue = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    prop.SetValue(obj, longValue);
                    offset += sizeof(long);
                    break;

                case TypeCode.Single:
                    var floatValue = Unsafe.ReadUnaligned<float>(ref buffer[offset]);
                    prop.SetValue(obj, floatValue);
                    offset += sizeof(float);
                    break;

                case TypeCode.Double:
                    var doubleValue = Unsafe.ReadUnaligned<double>(ref buffer[offset]);
                    prop.SetValue(obj, doubleValue);
                    offset += sizeof(double);
                    break;

                default:
                    if (prop.PropertyType == typeof(TimeSpan))
                    {
                        var timeSpanTicks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                        prop.SetValue(obj, new TimeSpan(timeSpanTicks));
                        offset += sizeof(long);
                    }
                    else if (prop.PropertyType == typeof(Guid))
                    {
                        var guidBytes = new byte[16];
                        for (var i = 0; i < 16; i++)
                        {
                            guidBytes[i] = buffer[offset + i];
                        }

                        prop.SetValue(obj, new Guid(guidBytes));
                        offset += 16;
                    }
                    else if (prop.PropertyType == typeof(byte[]))
                    {
                        var lengthAttribute = prop.GetCustomAttribute<FixedLengthStringAttribute>();
                        if (lengthAttribute == null)
                        {
                            throw new Exception("The byte[] property must have a FixedLengthStringAttribute.");
                        }

                        var bytes = new byte[lengthAttribute.BytesLength];
                        for (var i = 0; i < lengthAttribute.BytesLength; i++)
                        {
                            bytes[i] = buffer[offset + i];
                        }

                        prop.SetValue(obj, bytes);
                        offset += lengthAttribute.BytesLength;
                    }
                    else
                    {
                        throw new Exception($"Unsupported property type: {prop.PropertyType}");
                    }

                    break;
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

    private void WriteBulk(long index, T[] objs)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject * objs.Length];
        // Save the strings.
        var objWithStrings = SaveObjectStrings(objs);

        // Sequential write. Serialize objects in parallel.
        Parallel.For(0, objs.Length, i => { SerializeBytes(objWithStrings[i], buffer, sizeOfObject * i); });

        // Sequential write. Write binary data to disk.
        StructureFileAccess.WriteInFile(sizeOfObject * index + CountMarkerSize, buffer);
    }

    /// <summary>
    /// Add objects in bulk.
    ///
    /// This method is thread-safe. You can call it from multiple threads.
    /// </summary>
    /// <param name="objs">Array of objects to add in bulk.</param>
    public void Add(params T[] objs)
    {
        var indexToWrite = ProvisionWriteSpaceAndGetStartOffset(objs.Length);
        WriteBulk(indexToWrite, objs);
        Interlocked.Increment(ref BulkAppendCount);
        SetArchivedAsProvisioned();
    }

    public T Read(int index)
    {
        if (index < 0 || index >= ArchivedItemsCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        var sizeOfObject = GetItemSize();
        var data = StructureFileAccess.ReadInFile(sizeOfObject * index + CountMarkerSize, sizeOfObject);
        Interlocked.Increment(ref ReadCount);
        return DeserializeBytes(data);
    }

    /// <summary>
    /// Read objects in bulk.
    /// 
    /// This method is thread-safe. You can call it from multiple threads.
    /// </summary>
    /// <param name="indexFrom">Start index of the objects to read.</param>
    /// <param name="take">Number of objects to read.</param>
    /// <returns>An array of objects read from the file.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the indexFrom is less than 0 or indexFrom + count is greater than the total number of objects.</exception>
    public T[] ReadBulk(int indexFrom, int take)
    {
        if (indexFrom < 0 || indexFrom + take > ArchivedItemsCount)
        {
            throw new ArgumentOutOfRangeException(nameof(indexFrom));
        }

        var sizeOfObject = GetItemSize();
        // Sequential read. Load binary data from disk and deserialize them in parallel.
        var data = StructureFileAccess.ReadInFile(sizeOfObject * indexFrom + CountMarkerSize, sizeOfObject * take);
        var result = new T[take];
        Parallel.For(0, take, i =>
        {
            // TODO: Optimize: We need to preload the string file, because the string file is accessed randomly.
            // Random access to the string file to load the string data.
            result[i] = DeserializeBytes(data, sizeOfObject * i);
        });
        Interlocked.Increment(ref ReadBulkCount);
        return result;
    }
    
    public async Task DeleteAsync()
    {
        await StructureFileAccess.DeleteAsync();
        await StringRepository.DeleteAsync();
    }
}

// Define a class to hold the object and its persisted strings.
public class ObjectWithPersistedStrings
{
    public required BucketItem Object { get; init; }
    public required IEnumerable<SavedString> Strings { get; init; }
}

public enum BucketItemPropertyType
{
    Int32,
    Boolean,
    String,
    DateTime,
    Int64,
    Single,
    Double,
    TimeSpan,
    Guid,
    FixedSizeByteArray
}

public class BucketItemTypeDefinition
{
    public required IReadOnlyDictionary<string, BucketItemPropertyType> Properties { get; init; }
    public required IReadOnlyDictionary<string, int> FixedByteArrayLengths { get; init; }
}

public class BucketItem
{
    public Dictionary<string, BucketItemProperty<object>> Properties { get; set; } = new();
}

public class BucketItemProperty<T>
{
    public T? Value { get; set; }
    public BucketItemPropertyType Type { get; set; }
}

public interface IDynamicObjectBucket
{
    int Count { get; }
    void Add(params BucketItem[] objs);
    BucketItem Read(int index);
    BucketItem[] ReadBulk(int indexFrom, int take);
    Task DeleteAsync();
    string OutputStatistics();
    Task SyncAsync();
}

public class DynamicObjectBucket : IDynamicObjectBucket
{
    public int Count => ArchivedItemsCount;
    public int SpaceProvisionedItemsCount { get; private set; }
    public int ArchivedItemsCount { get; private set; }

    private const int CountMarkerSize = sizeof(int) + sizeof(int);
    private readonly object _expandLengthLock = new();

    public readonly CachedFileAccessService StructureFileAccess;
    public readonly StringRepository.ObjectStorage.StringRepository StringRepository;

    // Statistics
    public int SingleAppendCount;
    public int BulkAppendCount;
    public int ReadCount;
    public int ReadBulkCount;

    private readonly BucketItemTypeDefinition _itemTypeDefinition;

    public DynamicObjectBucket(
        BucketItemTypeDefinition itemTypeDefinition,
        string structureFilePath,
        string stringFilePath,
        long initialSizeIfNotExists = Consts.Consts.DefaultPhysicalFileSize,
        int cachePageSize = Consts.Consts.ReadCachePageSize,
        int maxCachedPagesCount = Consts.Consts.MaxReadCachedPagesCount,
        int hotCacheItems = Consts.Consts.ReadCacheHotCacheItems)
    {
        _itemTypeDefinition = itemTypeDefinition;

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

        (SpaceProvisionedItemsCount, ArchivedItemsCount) = GetItemsCount();
        if (SpaceProvisionedItemsCount != ArchivedItemsCount)
        {
            throw new Exception("The space provisioned items count and archived items count are not equal. The file may be corrupted. Is the process crashed in the middle of writing?");
        }
    }

    private (int provisioned, int archived) GetItemsCount()
    {
        var provisionedAndArchived = StructureFileAccess.ReadInFile(0, CountMarkerSize);
        var provisioned = BitConverter.ToInt32(provisionedAndArchived, 0);
        var archived = BitConverter.ToInt32(provisionedAndArchived, sizeof(int));
        return (provisioned, archived);
    }

    private void SaveCount(int provisioned, int archived)
    {
        var provisionedBytes = BitConverter.GetBytes(provisioned);
        var archivedBytes = BitConverter.GetBytes(archived);
        var buffer = new byte[CountMarkerSize];
        provisionedBytes.CopyTo(buffer, 0);
        archivedBytes.CopyTo(buffer, sizeof(int));
        StructureFileAccess.WriteInFile(0, buffer);
    }

    private long ProvisionWriteSpaceAndGetStartOffset(int itemsCount)
    {
        long writeOffset;
        lock (_expandLengthLock)
        {
            writeOffset = SpaceProvisionedItemsCount;
            SpaceProvisionedItemsCount += itemsCount;
            SaveCount(SpaceProvisionedItemsCount, ArchivedItemsCount);
        }

        return writeOffset;
    }

    private void SetArchivedAsProvisioned()
    {
        lock (_expandLengthLock)
        {
            ArchivedItemsCount = SpaceProvisionedItemsCount;
            SaveCount(SpaceProvisionedItemsCount, ArchivedItemsCount);
        }
    }

    private int GetItemSize()
    {
        var size = 0;
        foreach (var prop in _itemTypeDefinition.Properties)
        {
            var propertyName = prop.Key;
            var propertyType = prop.Value;

            switch (propertyType)
            {
                case BucketItemPropertyType.Int32:
                    size += sizeof(int);
                    break;
                case BucketItemPropertyType.Boolean:
                    size += sizeof(bool);
                    break;
                case BucketItemPropertyType.String:
                    size += sizeof(long) + sizeof(int); // Offset as long and Length as int.
                    break;
                case BucketItemPropertyType.DateTime:
                    size += sizeof(long); // DateTime.Ticks is stored as long.
                    break;
                case BucketItemPropertyType.Int64:
                    size += sizeof(long);
                    break;
                case BucketItemPropertyType.Single:
                    size += sizeof(float);
                    break;
                case BucketItemPropertyType.Double:
                    size += sizeof(double);
                    break;
                case BucketItemPropertyType.TimeSpan:
                    size += sizeof(long); // TimeSpan.Ticks is stored as long.
                    break;
                case BucketItemPropertyType.Guid:
                    size += 16; // Guid occupies 16 bytes.
                    break;
                case BucketItemPropertyType.FixedSizeByteArray:
                    if (_itemTypeDefinition.FixedByteArrayLengths.TryGetValue(propertyName, out var length))
                    {
                        size += length;
                    }
                    else
                    {
                        throw new Exception($"The FixedSizeByteArray property '{propertyName}' must have a defined length.");
                    }
                    break;
                default:
                    throw new Exception($"Unsupported property type: {propertyType}");
            }
        }

        return size;
    }

    private ObjectWithPersistedStrings[] SaveObjectStrings(BucketItem[] objs)
    {
        // Get the list of string property names
        var stringPropertyNames = _itemTypeDefinition.Properties
            .Where(p => p.Value == BucketItemPropertyType.String)
            .Select(p => p.Key)
            .ToArray();

        var stringsCount = stringPropertyNames.Length;

        // Collect all strings
        var strings = new byte[objs.Length * stringsCount][];

        Parallel.For(0, objs.Length, i =>
        {
            var obj = objs[i];
            for (var j = 0; j < stringsCount; j++)
            {
                var propertyName = stringPropertyNames[j];
                if (obj.Properties.TryGetValue(propertyName, out var property) && property.Value is string str)
                {
                    strings[i * stringsCount + j] = Encoding.UTF8.GetBytes(str);
                }
                else
                {
                    strings[i * stringsCount + j] = Array.Empty<byte>();
                }
            }
        });

        // Save strings and get offsets
        var savedStrings = StringRepository.BulkWriteStringContentAndGetOffsets(strings);

        // Create ObjectWithPersistedStrings array
        var result = new ObjectWithPersistedStrings[objs.Length];

        Parallel.For(0, objs.Length, i =>
        {
            var objWithStrings = new ObjectWithPersistedStrings
            {
                Object = objs[i],
                Strings = savedStrings.Skip(i * stringsCount).Take(stringsCount)
            };
            result[i] = objWithStrings;
        });

        return result;
    }

    private void SerializeBytes(ObjectWithPersistedStrings objWithStrings, byte[] buffer, int offset)
    {
        var obj = objWithStrings.Object;
        var stringsEnumerator = objWithStrings.Strings.GetEnumerator();
        using var stringsEnumerator1 = stringsEnumerator as IDisposable;
        stringsEnumerator.MoveNext();

        foreach (var propDef in _itemTypeDefinition.Properties)
        {
            var propertyName = propDef.Key;
            var propertyType = propDef.Value;
            var propertyValue = obj.Properties.TryGetValue(propertyName, out var property) ? property.Value : null;

            switch (propertyType)
            {
                case BucketItemPropertyType.Int32:
                    Unsafe.WriteUnaligned(ref buffer[offset], Convert.ToInt32(propertyValue));
                    offset += sizeof(int);
                    break;
                case BucketItemPropertyType.Boolean:
                    Unsafe.WriteUnaligned(ref buffer[offset], Convert.ToBoolean(propertyValue) ? (byte)1 : (byte)0);
                    offset += sizeof(bool);
                    break;
                case BucketItemPropertyType.String:
                    // For string, write the offset and length from savedStrings
                    var savedString = stringsEnumerator.Current;
                    if (!stringsEnumerator.MoveNext() && objWithStrings.Strings.Any())
                        throw new Exception("Mismatch in number of strings to serialize.");

                    Unsafe.WriteUnaligned(ref buffer[offset], savedString.Offset);
                    offset += sizeof(long);
                    Unsafe.WriteUnaligned(ref buffer[offset], savedString.Length);
                    offset += sizeof(int);
                    break;
                case BucketItemPropertyType.DateTime:
                    var dateTimeTicks = propertyValue != null ? ((DateTime)propertyValue).Ticks : 0L;
                    Unsafe.WriteUnaligned(ref buffer[offset], dateTimeTicks);
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Int64:
                    Unsafe.WriteUnaligned(ref buffer[offset], Convert.ToInt64(propertyValue));
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Single:
                    Unsafe.WriteUnaligned(ref buffer[offset], Convert.ToSingle(propertyValue));
                    offset += sizeof(float);
                    break;
                case BucketItemPropertyType.Double:
                    Unsafe.WriteUnaligned(ref buffer[offset], Convert.ToDouble(propertyValue));
                    offset += sizeof(double);
                    break;
                case BucketItemPropertyType.TimeSpan:
                    var timeSpanTicks = propertyValue != null ? ((TimeSpan)propertyValue).Ticks : 0L;
                    Unsafe.WriteUnaligned(ref buffer[offset], timeSpanTicks);
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Guid:
                    var guidBytes = propertyValue != null ? ((Guid)propertyValue).ToByteArray() : new byte[16];
                    for (var i = 0; i < 16; i++)
                    {
                        buffer[offset + i] = guidBytes[i];
                    }
                    offset += 16;
                    break;
                case BucketItemPropertyType.FixedSizeByteArray:
                    if (_itemTypeDefinition.FixedByteArrayLengths.TryGetValue(propertyName, out var length))
                    {
                        var bytes = propertyValue as byte[] ?? Array.Empty<byte>();
                        if (bytes.Length > length)
                        {
                            throw new Exception($"The byte[] property '{propertyName}' is too long.");
                        }
                        Array.Copy(bytes, 0, buffer, offset, bytes.Length);
                        offset += length;
                    }
                    else
                    {
                        throw new Exception($"The FixedSizeByteArray property '{propertyName}' must have a defined length.");
                    }
                    break;
                default:
                    throw new Exception($"Unsupported property type: {propertyType}");
            }
        }
    }

    private BucketItem DeserializeBytes(byte[] buffer, int offset = 0)
    {
        var obj = new BucketItem();
        obj.Properties = new Dictionary<string, BucketItemProperty<object>>();

        var stringsToLoad = new List<(string PropertyName, long Offset, int Length)>();

        foreach (var propDef in _itemTypeDefinition.Properties)
        {
            var propertyName = propDef.Key;
            var propertyType = propDef.Value;

            switch (propertyType)
            {
                case BucketItemPropertyType.Int32:
                    var intValue = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = intValue,
                        Type = propertyType
                    };
                    offset += sizeof(int);
                    break;
                case BucketItemPropertyType.Boolean:
                    var boolValue = Unsafe.ReadUnaligned<byte>(ref buffer[offset]) != 0;
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = boolValue,
                        Type = propertyType
                    };
                    offset += sizeof(bool);
                    break;
                case BucketItemPropertyType.String:
                    var strOffset = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    offset += sizeof(long);
                    var strLength = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                    offset += sizeof(int);

                    // Defer loading the string until after we have processed all properties
                    stringsToLoad.Add((propertyName, strOffset, strLength));
                    break;
                case BucketItemPropertyType.DateTime:
                    var dateTimeTicks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = new DateTime(dateTimeTicks),
                        Type = propertyType
                    };
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Int64:
                    var longValue = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = longValue,
                        Type = propertyType
                    };
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Single:
                    var floatValue = Unsafe.ReadUnaligned<float>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = floatValue,
                        Type = propertyType
                    };
                    offset += sizeof(float);
                    break;
                case BucketItemPropertyType.Double:
                    var doubleValue = Unsafe.ReadUnaligned<double>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = doubleValue,
                        Type = propertyType
                    };
                    offset += sizeof(double);
                    break;
                case BucketItemPropertyType.TimeSpan:
                    var timeSpanTicks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = new TimeSpan(timeSpanTicks),
                        Type = propertyType
                    };
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Guid:
                    var guidBytes = new byte[16];
                    for (var i = 0; i < 16; i++)
                    {
                        guidBytes[i] = buffer[offset + i];
                    }
                    obj.Properties[propertyName] = new BucketItemProperty<object>
                    {
                        Value = new Guid(guidBytes),
                        Type = propertyType
                    };
                    offset += 16;
                    break;
                case BucketItemPropertyType.FixedSizeByteArray:
                    if (_itemTypeDefinition.FixedByteArrayLengths.TryGetValue(propertyName, out var length))
                    {
                        var bytes = new byte[length];
                        Array.Copy(buffer, offset, bytes, 0, length);
                        obj.Properties[propertyName] = new BucketItemProperty<object>
                        {
                            Value = bytes,
                            Type = propertyType
                        };
                        offset += length;
                    }
                    else
                    {
                        throw new Exception($"The FixedSizeByteArray property '{propertyName}' must have a defined length.");
                    }
                    break;
                default:
                    throw new Exception($"Unsupported property type: {propertyType}");
            }
        }

        // Now load the strings
        foreach (var strInfo in stringsToLoad)
        {
            var str = StringRepository.LoadStringContent(strInfo.Offset, strInfo.Length);
            obj.Properties[strInfo.PropertyName] = new BucketItemProperty<object>
            {
                Value = str,
                Type = BucketItemPropertyType.String
            };
        }

        return obj;
    }

    public void Add(params BucketItem[] objs)
    {
        var indexToWrite = ProvisionWriteSpaceAndGetStartOffset(objs.Length);
        WriteBulk(indexToWrite, objs);
        Interlocked.Increment(ref BulkAppendCount);
        SetArchivedAsProvisioned();
    }

    private void WriteBulk(long index, BucketItem[] objs)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject * objs.Length];

        // Save the strings.
        var objWithStrings = SaveObjectStrings(objs);

        // Serialize objects in parallel.
        Parallel.For(0, objs.Length, i =>
        {
            SerializeBytes(objWithStrings[i], buffer, sizeOfObject * i);
        });

        // Write binary data to disk.
        StructureFileAccess.WriteInFile(sizeOfObject * index + CountMarkerSize, buffer);
    }

    public BucketItem Read(int index)
    {
        if (index < 0 || index >= ArchivedItemsCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        var sizeOfObject = GetItemSize();
        var data = StructureFileAccess.ReadInFile(sizeOfObject * index + CountMarkerSize, sizeOfObject);
        Interlocked.Increment(ref ReadCount);
        return DeserializeBytes(data);
    }

    public BucketItem[] ReadBulk(int indexFrom, int take)
    {
        if (indexFrom < 0 || indexFrom + take > ArchivedItemsCount)
        {
            throw new ArgumentOutOfRangeException(nameof(indexFrom));
        }

        var sizeOfObject = GetItemSize();
        // Load binary data from disk and deserialize them in parallel.
        var data = StructureFileAccess.ReadInFile(sizeOfObject * indexFrom + CountMarkerSize, sizeOfObject * take);
        var result = new BucketItem[take];
        Parallel.For(0, take, i =>
        {
            result[i] = DeserializeBytes(data, sizeOfObject * i);
        });
        Interlocked.Increment(ref ReadBulkCount);
        return result;
    }

    public async Task DeleteAsync()
    {
        await StructureFileAccess.DeleteAsync();
        await StringRepository.DeleteAsync();
    }

    public string OutputStatistics()
    {
        long spaceProvisionedItemsCount;
        lock (_expandLengthLock)
        {
            spaceProvisionedItemsCount = SpaceProvisionedItemsCount;
        }
        // ReSharper disable once InconsistentlySynchronizedField
        return $@"
Object repository with dynamic item type statistics:

* Space provisioned items count: {spaceProvisionedItemsCount}
* Archived items count: {ArchivedItemsCount}
* Consumed actual storage space: {GetItemSize() * spaceProvisionedItemsCount} bytes
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

    public Task SyncAsync()
    {
        // You don't need to sync the string repository, because the string repository is always in sync with the structure file.
        return Task.CompletedTask;
    }
}
