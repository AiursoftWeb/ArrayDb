using System.Collections.Concurrent;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.WriteBuffer;

namespace Aiursoft.ArrayDb.Partitions;

public class PartitionedObjectBucket<T, TK> where T : PartitionedBucketEntity<TK>, new() where TK : notnull
{
    private Dictionary<TK, BufferedObjectBuckets<T>> Partitions { get; } = new();
    
    
    private readonly object _partitionsLock = new();
    private readonly string _databaseName;
    private readonly string _databaseDirectory;
    private readonly long _initialSizeIfNotExists;
    private readonly int _cachePageSize;
    private readonly int _maxCachedPagesCount;
    private readonly int _hotCacheItems;
    private readonly int _maxBufferedItemsCount;
    private readonly int _initialCooldownMilliseconds;
    private readonly int _maxCooldownMilliseconds;

    public PartitionedObjectBucket(string databaseName,
        string databaseDirectory,
        long initialSizeIfNotExists = Consts.Consts.DefaultPhysicalFileSize,
        int cachePageSize = Consts.Consts.ReadCachePageSize,
        int maxCachedPagesCount = Consts.Consts.MaxReadCachedPagesCount,
        int hotCacheItems = Consts.Consts.ReadCacheHotCacheItems,
        int maxBufferedItemsCount = Consts.Consts.MaxWriteBufferCachedItemsCount,
        int initialCooldownMilliseconds = Consts.Consts.WriteBufferInitialCooldownMilliseconds,
        int maxCooldownMilliseconds = Consts.Consts.WriteBufferMaxCooldownMilliseconds)
    {
        _databaseName = databaseName;
        _databaseDirectory = databaseDirectory;
        _initialSizeIfNotExists = initialSizeIfNotExists;
        _cachePageSize = cachePageSize;
        _maxCachedPagesCount = maxCachedPagesCount;
        _hotCacheItems = hotCacheItems;
        _maxBufferedItemsCount = maxBufferedItemsCount;
        _initialCooldownMilliseconds = initialCooldownMilliseconds;
        _maxCooldownMilliseconds = maxCooldownMilliseconds;
        
        // Init partitions based on existing files
        var files = Directory.GetFiles(databaseDirectory);
        foreach (var file in files)
        {
            var fileName = Path.GetFileName(file);
            if (fileName.StartsWith(databaseName) && fileName.EndsWith("_structure.dat"))
            {
                var partitionId = fileName.Substring(databaseName.Length + 1, fileName.Length - databaseName.Length - 1 - "_structure.dat".Length);
                var parrtitionIdTk = (TK)Convert.ChangeType(partitionId, typeof(TK));
                Partitions[parrtitionIdTk] = new BufferedObjectBuckets<T>(
                    new ObjectBucket.ObjectBucket<T>(
                        file,
                        Path.Combine(databaseDirectory, $"{databaseName}_{partitionId}_string.dat"),
                        initialSizeIfNotExists,
                        cachePageSize,
                        maxCachedPagesCount,
                        hotCacheItems),
                    maxBufferedItemsCount,
                    initialCooldownMilliseconds,
                    maxCooldownMilliseconds);
            }
        }
    }

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

            var structureFilePath = Path.Combine(_databaseDirectory, $"{_databaseName}_{partitionId}_structure.dat");
            var stringFilePath = Path.Combine(_databaseDirectory, $"{_databaseName}_{partitionId}_string.dat");
            Partitions[partitionId] = new BufferedObjectBuckets<T>(
                new ObjectBucket.ObjectBucket<T>(
                    structureFilePath,
                    stringFilePath,
                    _initialSizeIfNotExists,
                    _cachePageSize,
                    _maxCachedPagesCount,
                    _hotCacheItems),
                maxBufferedItemsCount: _maxBufferedItemsCount,
                initialCooldownMilliseconds: _initialCooldownMilliseconds,
                maxCooldownMilliseconds: _maxCooldownMilliseconds);

            return Partitions[partitionId];
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

    public int Count()
    {
        var totalItemsCount = 0;
        foreach (var partition in Partitions)
        {
            totalItemsCount += partition.Value.InnerBucket.ArchivedItemsCount;
        }
        return totalItemsCount;
    }
    
    public int Count(TK partitionKey)
    {
        return GetPartitionById(partitionKey).InnerBucket.ArchivedItemsCount;
    }
    
    public async Task DeletePartitionAsync(TK partitionKey)
    {
        if (Partitions.TryGetValue(partitionKey, out var partition))
        {
            await partition.InnerBucket.DeleteAsync();
            Partitions.Remove(partitionKey);
        }
        else
        {
            throw new InvalidOperationException($"Partition {partitionKey} not found!");
        }
    }
    
    public IEnumerable<T> AsEnumerable(TK partitionKey, int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize)
    {
        return GetPartitionById(partitionKey).InnerBucket.AsEnumerable(bufferedReadPageSize)
            .Select(item =>
            {
                item.PartitionId = partitionKey;
                return item;
            });
    }

    public Task SyncAsync()
    {
        var allSyncTasks = Partitions.Select(partition => partition.Value.SyncAsync()).ToArray();
        return Task.WhenAll(allSyncTasks);
    }
}