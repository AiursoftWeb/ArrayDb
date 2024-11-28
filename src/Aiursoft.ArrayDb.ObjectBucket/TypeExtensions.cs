using System.Reflection;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Attributes;

namespace Aiursoft.ArrayDb.ObjectBucket;

public static class TypeExtensions
{
    public static PropertyInfo[] GetPropertiesShouldPersistOnDisk(this Type type)
    {
        return type.GetProperties()
            .Where(p => p is { CanRead: true, CanWrite: true })
            .Where(p => Type.GetTypeCode(p.PropertyType) switch
            {
                TypeCode.Int32 => true,
                TypeCode.Boolean => true,
                TypeCode.String => true,
                TypeCode.DateTime => true,
                TypeCode.Int64 => true,
                TypeCode.Single => true,
                TypeCode.Double => true,
                _ => p.PropertyType == typeof(TimeSpan) || p.PropertyType == typeof(Guid) || p.PropertyType == typeof(byte[])
            })
            .Where(p => p.GetCustomAttributes(typeof(PartitionKeyAttribute), false).Length == 0)
            .OrderBy(p => p.Name)
            .ToArray();
    }
    
    public static byte[] TrimEndZeros(this byte[] bytes)
    {
        var length = bytes.Length;
        while (length > 0 && bytes[length - 1] == 0)
        {
            length--;
        }

        var result = new byte[length];
        Array.Copy(bytes, result, length);
        return result;
    }
}