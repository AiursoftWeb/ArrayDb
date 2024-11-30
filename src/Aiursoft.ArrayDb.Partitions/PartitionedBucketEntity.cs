using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Attributes;

namespace Aiursoft.ArrayDb.Partitions;

public abstract class PartitionedBucketEntity<T> 
{
    [PartitionKey]
    public virtual T PartitionId { get; set; } = default!;
}
