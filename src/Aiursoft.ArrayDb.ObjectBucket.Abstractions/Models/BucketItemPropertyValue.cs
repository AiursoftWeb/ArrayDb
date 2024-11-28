namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;

public class BucketItemPropertyValue<T>
{
    public T? Value { get; set; }
    public BucketItemPropertyType Type { get; set; }
}