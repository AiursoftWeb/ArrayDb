using System.Runtime.CompilerServices;
using System.Text;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ReadLruCache;

namespace Aiursoft.ArrayDb.ObjectBucket.Dynamic;

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
            throw new Exception($"The space provisioned items count and archived items count are not equal. The file may be corrupted. Is the process crashed in the middle of writing? (SpaceProvisionedItemsCount: {SpaceProvisionedItemsCount}, ArchivedItemsCount: {ArchivedItemsCount})");
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

    private BucketItemWithPersistedStrings[] SaveObjectStrings(BucketItem[] objs)
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
        var result = new BucketItemWithPersistedStrings[objs.Length];

        Parallel.For(0, objs.Length, i =>
        {
            var objWithStrings = new BucketItemWithPersistedStrings
            {
                Object = objs[i],
                Strings = savedStrings.Skip(i * stringsCount).Take(stringsCount)
            };
            result[i] = objWithStrings;
        });

        return result;
    }

    private void SerializeBytes(BucketItemWithPersistedStrings objWithStrings, byte[] buffer, int offset)
    {
        var obj = objWithStrings.Object;
        using var stringsEnumerator = objWithStrings.Strings.GetEnumerator();
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
                    stringsEnumerator.MoveNext();

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
        obj.Properties = new Dictionary<string, BucketItemPropertyValue<object>>();

        var stringsToLoad = new List<(string PropertyName, long Offset, int Length)>();

        foreach (var propDef in _itemTypeDefinition.Properties)
        {
            var propertyName = propDef.Key;
            var propertyType = propDef.Value;

            switch (propertyType)
            {
                case BucketItemPropertyType.Int32:
                    var intValue = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
                    {
                        Value = intValue,
                        Type = propertyType
                    };
                    offset += sizeof(int);
                    break;
                case BucketItemPropertyType.Boolean:
                    var boolValue = Unsafe.ReadUnaligned<byte>(ref buffer[offset]) != 0;
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
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
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
                    {
                        Value = new DateTime(dateTimeTicks),
                        Type = propertyType
                    };
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Int64:
                    var longValue = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
                    {
                        Value = longValue,
                        Type = propertyType
                    };
                    offset += sizeof(long);
                    break;
                case BucketItemPropertyType.Single:
                    var floatValue = Unsafe.ReadUnaligned<float>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
                    {
                        Value = floatValue,
                        Type = propertyType
                    };
                    offset += sizeof(float);
                    break;
                case BucketItemPropertyType.Double:
                    var doubleValue = Unsafe.ReadUnaligned<double>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
                    {
                        Value = doubleValue,
                        Type = propertyType
                    };
                    offset += sizeof(double);
                    break;
                case BucketItemPropertyType.TimeSpan:
                    var timeSpanTicks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
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
                    obj.Properties[propertyName] = new BucketItemPropertyValue<object>
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
                        obj.Properties[propertyName] = new BucketItemPropertyValue<object>
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
            obj.Properties[strInfo.PropertyName] = new BucketItemPropertyValue<object>
            {
                Value = str,
                Type = BucketItemPropertyType.String
            };
        }

        return obj;
    }

    public void Add(params BucketItem[] objs)
    {
        // Provision space for the objects. This method is thread-safe.
        var indexToWrite = ProvisionWriteSpaceAndGetStartOffset(objs.Length);
        
        // Save the strings.
        var objWithStrings = SaveObjectStrings(objs);

        // Allocate buffer for the objects.
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject * objs.Length];

        // Serialize objects in parallel.
        Parallel.For(0, objs.Length, i =>
        {
            SerializeBytes(objWithStrings[i], buffer, sizeOfObject * i);
        });

        // Write binary data to disk.
        StructureFileAccess.WriteInFile(sizeOfObject * indexToWrite + CountMarkerSize, buffer);
        
        // Update statistics.
        Interlocked.Increment(ref BulkAppendCount);
        
        // Set the archived count as the provisioned count.
        SetArchivedAsProvisioned();
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