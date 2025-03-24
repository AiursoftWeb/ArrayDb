using System.Dynamic;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

namespace Aiursoft.ArrayDb.ObjectBucket.Dynamic.Simplify;

/// <summary>
/// Dynamic wrapper for BucketItem that provides property-like access
/// </summary>
public class SimplifiedBucketItem(BucketItem item) : DynamicObject
{
    // Support for property access like item.PropertyName
    public override bool TryGetMember(GetMemberBinder binder, out object? result)
    {
        var propertyName = binder.Name;

        if (item.Properties.TryGetValue(propertyName, out var property))
        {
            // Return the appropriate type based on the property type
            result = ConvertValueToType(property.Value, property.Type);
            return true;
        }

        result = null;
        return false;
    }

    // Support for indexer access like item["PropertyName"]
    public override bool TryGetIndex(GetIndexBinder binder, object[] indexes, out object? result)
    {
        if (indexes.Length == 1 && indexes[0] is string propertyName)
        {
            if (item.Properties.TryGetValue(propertyName, out var property))
            {
                result = ConvertValueToType(property.Value, property.Type);
                return true;
            }
        }

        result = null;
        return false;
    }

    private object? ConvertValueToType(object? value, BucketItemPropertyType type)
    {
        if (value == null) return null;

        return type switch
        {
            BucketItemPropertyType.Int32 => Convert.ToInt32(value),
            BucketItemPropertyType.Boolean => Convert.ToBoolean(value),
            BucketItemPropertyType.String => value.ToString(),
            BucketItemPropertyType.DateTime => value is DateTime dt ? dt : throw new InvalidCastException(),
            BucketItemPropertyType.Int64 => Convert.ToInt64(value),
            BucketItemPropertyType.Single => Convert.ToSingle(value),
            BucketItemPropertyType.Double => Convert.ToDouble(value),
            BucketItemPropertyType.TimeSpan => value is TimeSpan ts ? ts : throw new InvalidCastException(),
            BucketItemPropertyType.Guid => value is Guid g ? g : throw new InvalidCastException(),
            BucketItemPropertyType.FixedSizeByteArray => value as byte[],
            _ => value
        };
    }

    // Access to the original item if needed
    public BucketItem OriginalItem => item;
}
