using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Attributes;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;

namespace Aiursoft.ArrayDb.ObjectBucket;

// Strong typed.
/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ObjectBucket<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T> : IObjectBucket<T>
    where T : new()
{
    private readonly DynamicObjectBucket _dynamicBucket;

    public int Count => _dynamicBucket.Count;
    public int SpaceProvisionedItemsCount => _dynamicBucket.SpaceProvisionedItemsCount;
    public int ArchivedItemsCount => _dynamicBucket.ArchivedItemsCount;

    /// <summary>
    /// Initializes a new instance of the ObjectBucket class.
    /// </summary>
    /// <param name="structureFilePath">The path to the file that stores the structure of the objects.</param>
    /// <param name="stringFilePath">The path to the file that stores the string data.</param>
    /// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
    /// <param name="cachePageSize">The size of the cache page.</param>
    /// <param name="maxCachedPagesCount">The maximum number of pages cached in memory.</param>
    /// <param name="hotCacheItems">The number of most recent pages that are considered hot and will not be moved even if they are used.</param>
    public ObjectBucket(
        string structureFilePath,
        string stringFilePath,
        long initialSizeIfNotExists = Consts.Consts.DefaultPhysicalFileSize,
        int cachePageSize = Consts.Consts.ReadCachePageSize,
        int maxCachedPagesCount = Consts.Consts.MaxReadCachedPagesCount,
        int hotCacheItems = Consts.Consts.ReadCacheHotCacheItems)
    {
        var itemTypeDefinition = BuildBucketItemTypeDefinition(typeof(T));

        _dynamicBucket = new DynamicObjectBucket(
            itemTypeDefinition,
            structureFilePath,
            stringFilePath,
            initialSizeIfNotExists,
            cachePageSize,
            maxCachedPagesCount,
            hotCacheItems);
    }

    private static BucketItemTypeDefinition BuildBucketItemTypeDefinition(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type type)
    {
        var properties = new Dictionary<string, BucketItemPropertyType>();
        var fixedByteArrayLengths = new Dictionary<string, int>();

        foreach (var prop in type.GetPropertiesShouldPersistOnDisk())
        {
            BucketItemPropertyType propertyType;
            switch (Type.GetTypeCode(prop.PropertyType))
            {
                case TypeCode.Int32:
                    propertyType = BucketItemPropertyType.Int32;
                    break;
                case TypeCode.Boolean:
                    propertyType = BucketItemPropertyType.Boolean;
                    break;
                case TypeCode.String:
                    propertyType = BucketItemPropertyType.String;
                    break;
                case TypeCode.DateTime:
                    propertyType = BucketItemPropertyType.DateTime;
                    break;
                case TypeCode.Int64:
                    propertyType = BucketItemPropertyType.Int64;
                    break;
                case TypeCode.Single:
                    propertyType = BucketItemPropertyType.Single;
                    break;
                case TypeCode.Double:
                    propertyType = BucketItemPropertyType.Double;
                    break;
                default:
                    if (prop.PropertyType == typeof(TimeSpan))
                    {
                        propertyType = BucketItemPropertyType.TimeSpan;
                    }
                    else if (prop.PropertyType == typeof(Guid))
                    {
                        propertyType = BucketItemPropertyType.Guid;
                    }
                    else if (prop.PropertyType == typeof(byte[]))
                    {
                        propertyType = BucketItemPropertyType.FixedSizeByteArray;

                        var lengthAttribute = prop.GetCustomAttribute<FixedLengthStringAttribute>();
                        if (lengthAttribute == null)
                        {
                            throw new Exception(
                                $"The byte[] property '{prop.Name}' must have a FixedLengthStringAttribute.");
                        }

                        fixedByteArrayLengths[prop.Name] = lengthAttribute.BytesLength;
                    }
                    else
                    {
                        throw new Exception($"Unsupported property type: {prop.PropertyType}");
                    }

                    break;
            }

            properties[prop.Name] = propertyType;
        }

        return new BucketItemTypeDefinition
        {
            Properties = properties,
            FixedByteArrayLengths = fixedByteArrayLengths
        };
    }

    public void Add(params T[] objs)
    {
        var bucketItems = objs.Select(ConvertToBucketItem).ToArray();
        _dynamicBucket.Add(bucketItems);
    }

    public T Read(int index)
    {
        var bucketItem = _dynamicBucket.Read(index);
        return ConvertToT(bucketItem);
    }

    public T[] ReadBulk(int indexFrom, int take)
    {
        var bucketItems = _dynamicBucket.ReadBulk(indexFrom, take);
        return bucketItems.Select(ConvertToT).ToArray();
    }

    public async Task DeleteAsync()
    {
        await _dynamicBucket.DeleteAsync();
    }

    public string OutputStatistics()
    {
        return _dynamicBucket.OutputStatistics();
    }

    public Task SyncAsync()
    {
        return _dynamicBucket.SyncAsync();
    }

    // Conversion methods
    private BucketItem ConvertToBucketItem(T obj)
    {
        var bucketItem = new BucketItem();
        foreach (var prop in typeof(T).GetPropertiesShouldPersistOnDisk())
        {
            var value = prop.GetValue(obj);
            BucketItemPropertyType propertyType;

            switch (Type.GetTypeCode(prop.PropertyType))
            {
                case TypeCode.Int32:
                    propertyType = BucketItemPropertyType.Int32;
                    break;
                case TypeCode.Boolean:
                    propertyType = BucketItemPropertyType.Boolean;
                    break;
                case TypeCode.String:
                    propertyType = BucketItemPropertyType.String;
                    break;
                case TypeCode.DateTime:
                    propertyType = BucketItemPropertyType.DateTime;
                    break;
                case TypeCode.Int64:
                    propertyType = BucketItemPropertyType.Int64;
                    break;
                case TypeCode.Single:
                    propertyType = BucketItemPropertyType.Single;
                    break;
                case TypeCode.Double:
                    propertyType = BucketItemPropertyType.Double;
                    break;
                default:
                    if (prop.PropertyType == typeof(TimeSpan))
                    {
                        propertyType = BucketItemPropertyType.TimeSpan;
                    }
                    else if (prop.PropertyType == typeof(Guid))
                    {
                        propertyType = BucketItemPropertyType.Guid;
                    }
                    else if (prop.PropertyType == typeof(byte[]))
                    {
                        propertyType = BucketItemPropertyType.FixedSizeByteArray;
                    }
                    else
                    {
                        throw new Exception($"Unsupported property type: {prop.PropertyType}");
                    }

                    break;
            }

            bucketItem.Properties[prop.Name] = new BucketItemPropertyValue
            {
                Value = value,
                Type = propertyType
            };
        }

        return bucketItem;
    }

    private T ConvertToT(BucketItem bucketItem)
    {
        var obj = new T();
        foreach (var prop in typeof(T).GetPropertiesShouldPersistOnDisk())
        {
            if (bucketItem.Properties.TryGetValue(prop.Name, out var property))
            {
                prop.SetValue(obj, property.Value);
            }
        }

        return obj;
    }
}