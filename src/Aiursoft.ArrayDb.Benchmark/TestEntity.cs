using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Partitions;

namespace Aiursoft.ArrayDb.Benchmark;

public class TestEntity : PartitionedBucketEntity<int>
{
    [PartitionKey]
    public int ThreadId { get; set; }

    [PartitionKey]
    public override int PartitionId
    {
        get => ThreadId;
        set => ThreadId = value;
    }
    
    public int Id { get; init; }
    public string ChineseString { get; init; } = string.Empty;
    public int Id10Times { get; init; }
    public bool IsEven { get; init; }
    public string? NullableEnglishString { get; init; }
}