using Aiursoft.ArrayDb.ObjectBucket.Abstractions;

namespace Aiursoft.ArrayDb.ObjectBucket;

public class BucketItemTypeDefinition
{
    public required IReadOnlyDictionary<string, BucketItemPropertyType> Properties { get; init; }
    public required IReadOnlyDictionary<string, int> FixedByteArrayLengths { get; init; }
}