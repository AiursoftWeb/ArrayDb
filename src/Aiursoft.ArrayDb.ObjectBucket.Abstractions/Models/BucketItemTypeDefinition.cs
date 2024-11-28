namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

public class BucketItemTypeDefinition
{
    public required IReadOnlyDictionary<string, BucketItemPropertyType> Properties { get; init; }
    public required IReadOnlyDictionary<string, int> FixedByteArrayLengths { get; init; }
}