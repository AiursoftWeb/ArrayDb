namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Attributes;

public class FixedLengthStringAttribute : Attribute
{
    public int BytesLength { get; init; }
}