namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

public class BucketItemTypeDefinition
{
    public required IReadOnlyDictionary<string, BucketItemPropertyType> Properties { get; init; }
    public IReadOnlyDictionary<string, int> FixedByteArrayLengths { get; init; } = new Dictionary<string, int>();
}