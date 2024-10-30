using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Tests.Base.Models;

public class SampleData : BucketEntity
{
    public int MyNumber1 { get; init; }
    public string MyString1 { get; init; } = string.Empty;
    public int MyNumber2 { get; init; }
    public bool MyBoolean1 { get; init; }
    public string? MyString2 { get; init; }
}