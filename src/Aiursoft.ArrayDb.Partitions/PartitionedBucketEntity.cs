using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Partitions;

public abstract class PartitionedBucketEntity<T> : BucketEntity 
{
    [PartitionKey]
    public virtual T PartitionId { get; set; } = default!;
}
