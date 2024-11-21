using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.ObjectBucket.Attributes;

namespace Aiursoft.ArrayDb.Partitions;

public abstract class PartitionedBucketEntity<T> : BucketEntity 
{
    [PartitionKey]
    public virtual T PartitionId { get; set; } = default!;
}
