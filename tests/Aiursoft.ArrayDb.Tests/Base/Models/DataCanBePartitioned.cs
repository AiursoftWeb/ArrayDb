using Aiursoft.ArrayDb.Partitions;

namespace Aiursoft.ArrayDb.Tests.Base.Models;

public class DataCanBePartitioned : PartitionedBucketEntity<int>
{
    public int ThreadId { get; set; }

    public override int PartitionId
    {
        get => ThreadId;
        set => ThreadId = value;
    }
    
    public string? Message { get; set; }
}