namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions;

public class BucketItem
{
    public Dictionary<string, BucketItemProperty<object>> Properties { get; set; } = new();
}