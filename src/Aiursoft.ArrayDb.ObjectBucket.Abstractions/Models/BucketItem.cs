namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

public class BucketItem
{
    public Dictionary<string, BucketItemPropertyValue> Properties { get; set; } = new();
}