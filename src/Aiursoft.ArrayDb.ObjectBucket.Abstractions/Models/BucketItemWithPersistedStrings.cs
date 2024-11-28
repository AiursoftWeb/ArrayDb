namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

public class BucketItemWithPersistedStrings
{
    public required BucketItem Object { get; init; }
    public required IEnumerable<PersistedString> Strings { get; init; }
}