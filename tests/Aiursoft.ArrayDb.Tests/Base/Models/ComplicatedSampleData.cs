using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Tests.Base.Models;

public class ComplicatedSampleData
{
    public string MyString1 { get; init; } = string.Empty;
    public DateTime MyDateTime1 { get; init; }
    public long MyLong1 { get; init; }
    public float MyFloat1 { get; init; }
    public double MyDouble1 { get; init; }
    public TimeSpan MyTimeSpan1 { get; init; }
    public Guid MyGuid1 { get; init; }
}