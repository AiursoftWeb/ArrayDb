namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

public class BucketItem
{
    public Dictionary<string, BucketItemPropertyValue<object>> Properties { get; set; } = new();
}