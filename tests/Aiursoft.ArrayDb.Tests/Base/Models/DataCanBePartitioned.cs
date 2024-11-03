using Aiursoft.ArrayDb.ObjectBucket;
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