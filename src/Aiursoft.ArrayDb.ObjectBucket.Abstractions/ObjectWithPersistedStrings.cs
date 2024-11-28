namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions;

public class ObjectWithPersistedStrings
{
    public required BucketItem Object { get; init; }
    public required IEnumerable<SavedString> Strings { get; init; }
}