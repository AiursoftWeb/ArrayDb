using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Partitions;

public abstract class PartitionedBucketEntity<T> : BucketEntity where T : struct
{
    public abstract T PartitionId { get; set; }
}
