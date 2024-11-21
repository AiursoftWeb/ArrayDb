using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.ObjectBucket.Attributes;
using Aiursoft.ArrayDb.Partitions;

namespace Aiursoft.ArrayDb.Tests.Base.Models;

public class DataCanBePartitioned : PartitionedBucketEntity<int>
{
    [PartitionKey]
    public int ThreadId { get; set; }

    [PartitionKey]
    public override int PartitionId
    {
        get => ThreadId;
        set => ThreadId = value;
    }
    
    public string? Message { get; set; }
    public int Id { get; set; }
}

public class DataCanBePartitionedByString : PartitionedBucketEntity<string>
{
    [PartitionKey] public string ThreadId { get; set; } = string.Empty;

    [PartitionKey]
    public override string PartitionId
    {
        get => ThreadId;
        set => ThreadId = value;
    }
    
    public string? Message { get; set; }
    public int Id { get; set; }
}

public class DataWithDefaultPartition : PartitionedBucketEntity<int>
{
    public int Id { get; set; }
    public string? Message { get; set; }
}