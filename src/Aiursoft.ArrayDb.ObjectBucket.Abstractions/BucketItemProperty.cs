namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions;

public class BucketItemProperty<T>
{
    public T? Value { get; set; }
    public BucketItemPropertyType Type { get; set; }
}