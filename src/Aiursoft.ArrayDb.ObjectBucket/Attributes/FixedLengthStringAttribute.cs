namespace Aiursoft.ArrayDb.ObjectBucket.Attributes;

public class FixedLengthStringAttribute : Attribute
{
    public int BytesLength { get; init; }
}