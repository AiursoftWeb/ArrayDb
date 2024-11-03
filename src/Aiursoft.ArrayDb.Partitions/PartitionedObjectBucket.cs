using System.Collections.Concurrent;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.WriteBuffer;

namespace Aiursoft.ArrayDb.Partitions;

public class PartitionedObjectBucket<T, TK>(
    string databaseName,
    string databaseDirectory,
    long initialSizeIfNotExists = Consts.Consts.DefaultPhysicalFileSize,
    int cachePageSize = Consts.Consts.ReadCachePageSize,
    int maxCachedPagesCount = Consts.Consts.MaxReadCachedPagesCount,
    int hotCacheItems = Consts.Consts.ReadCacheHotCacheItems,
    int maxBufferedItemsCount = Consts.Consts.MaxWriteBufferCachedItemsCount,
    int initialCooldownMilliseconds = Consts.Consts.WriteBufferInitialCooldownMilliseconds,
    int maxCooldownMilliseconds = Consts.Consts.WriteBufferMaxCooldownMilliseconds)
    where T : PartitionedBucketEntity<TK>, new()
    where TK : struct
{
    private Dictionary<TK, BufferedObjectBuckets<T>> Partitions { get; } = new();
    private readonly object _partitionsLock = new();
    public int PartitionsCount => Partitions.Count;

    public string OutputStatistics()
    {
        var selfReport = $@"
Partitioned object buket with item type {typeof(T).Name} and partition key {typeof(TK).Name} statistics:

* Current partitions count: {Partitions.Count}

";
        foreach (var partition in Partitions)
        {
            selfReport += "Partition " + partition.Key + " statistics:\n";
            selfReport += partition.Value.OutputStatistics().AppendTabsEachLineHead() + "\n";
        }

        return selfReport;
    }

    // ReSharper disable once MemberCanBePrivate.Global
    public BufferedObjectBuckets<T> GetPartitionById(TK partitionId)
    {
        lock (_partitionsLock)
        {
            if (Partitions.TryGetValue(partitionId, out var partition))
            {
                return partition;
            }

            var structureFilePath = Path.Combine(databaseDirectory, $"{databaseName}_{partitionId}_structure.dat");
            var stringFilePath = Path.Combine(databaseDirectory, $"{databaseName}_{partitionId}_string.dat");
            var objectBucket = new ObjectBucket.ObjectBucket<T>(
                structureFilePath,
                stringFilePath,
                initialSizeIfNotExists,
                cachePageSize,
                maxCachedPagesCount,
                hotCacheItems);

            var buffer = new BufferedObjectBuckets<T>(
                objectBucket,
                maxBufferedItemsCount: maxBufferedItemsCount,
                initialCooldownMilliseconds: initialCooldownMilliseconds,
                maxCooldownMilliseconds: maxCooldownMilliseconds);
            Partitions[partitionId] = buffer;

            return buffer;
        }
    }

    public void Add(T obj)
    {
        var partition = GetPartitionById(obj.PartitionId);
        partition.AddBuffered(obj);
    }

    public void AddBulk(T[] objs)
    {
        var objsByPartition = objs.GroupBy(x => x.PartitionId);
        Parallel.ForEach(objsByPartition,
            partition => { GetPartitionById(partition.Key).AddBuffered(partition.ToArray()); });
    }

    [Obsolete(error: false, message: "Read objects one by one is slow. Use ReadBulk instead.")]
    public T Read(TK partitionKey, int index)
    {
        var item = GetPartitionById(partitionKey).InnerBucket.Read(index);
        item.PartitionId = partitionKey;
        return item;
    }

    public T[] ReadBulk(TK partitionKey, int indexFrom, int count)
    {
        var result = GetPartitionById(partitionKey).InnerBucket.ReadBulk(indexFrom, count);
        foreach (var item in result)
        {
            item.PartitionId = partitionKey;
        }
        return result;
    }

    public T[] ReadAll()
    {
        var results = new ConcurrentBag<T>();
        Parallel.ForEach(Partitions, partition =>
        {
            var partitionResults =
                partition.Value.InnerBucket.ReadBulk(0, partition.Value.InnerBucket.ArchivedItemsCount);
            foreach (var result in partitionResults)
            {
                result.PartitionId = partition.Key;
                results.Add(result);
            }
        });
        return results.ToArray();
    }

    public Task SyncAsync()
    {
        var allSyncTasks = Partitions.Select(partition => partition.Value.SyncAsync()).ToArray();
        return Task.WhenAll(allSyncTasks);
    }
}