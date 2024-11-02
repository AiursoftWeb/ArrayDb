using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Partitions;

public abstract class PartitionedBucketEntity<T> : BucketEntity where T : struct
{
    public virtual T PartitionId { get; set; }
}
