using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.WriteBuffer;

/// <summary>
/// Represents a class for managing buffered objects in buckets.
/// </summary>
/// <typeparam name="T">The type of objects to be stored in the buckets.</typeparam>
/// <param name="innerBucket">The underlying object bucket to store the objects.</param>
/// <param name="maxBufferedItemsCount">The maximum number of items that can be buffered before writing to the disk. Suggested value is maxCooldownMilliseconds * 1000. Because usual computer can insert around 1000 items per millisecond.</param>
/// <param name="initialCooldownMilliseconds">The initial cooldown time in milliseconds. Suggested value is 1000. Small value will cause data fragmentation. Large value will cause latency.</param>
/// <param name="maxCooldownMilliseconds">The maximum cooldown time in milliseconds. Suggested value is 1000 * 16. Because the queue may not be able to handle the high frequency of writes. According to the remaining tasks in the queue, increase the next cooldown time. But not more than 16 times the initial cooldown time.</param>
public class BufferedObjectBuckets<T>(
    ObjectBucket<T> innerBucket,
    int maxBufferedItemsCount = Consts.Consts.MaxWriteBufferCachedItemsCount,
    int initialCooldownMilliseconds = Consts.Consts.WriteBufferInitialCooldownMilliseconds,
    int maxCooldownMilliseconds = Consts.Consts.WriteBufferMaxCooldownMilliseconds)
    where T : BucketEntity, new()
{
    public ObjectBucket<T> InnerBucket => innerBucket;
    private readonly TasksQueue _tasksQueue = new();
    private readonly ConcurrentQueue<T> _buffer = [];
    private readonly object _bufferWriteLock = new();
    private readonly int _initialCooldownMilliseconds = initialCooldownMilliseconds;
    private int _cooldownMilliseconds = initialCooldownMilliseconds;
    
    // Statistics
    public int RequestHotWriteCount;
    public int RequestColdWriteCount;
    public int QueuedWriteCount;
    public int ActualWriteCount;
    public int CoolDownEventsCount;
    public readonly List<int> InsertItemsCountRecord = [];
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        RequestHotWriteCount = 0;
        RequestColdWriteCount = 0;
        QueuedWriteCount = 0;
        ActualWriteCount = 0;
        CoolDownEventsCount = 0;
        InsertItemsCountRecord.Clear();
    }
    
    public string OutputStatistics()
    {
        return $@"
Buffered object repository with item type {typeof(T).Name} statistics:

* Current instance status: {(IsHot ? "Hot" : "Cold")}
* Request write events count: {RequestHotWriteCount + RequestColdWriteCount}
* Requested hot write events count: {RequestHotWriteCount}
* Requested cold write events count: {RequestColdWriteCount}
* Queued write events count: {QueuedWriteCount}
* Actual write events count: {ActualWriteCount}
* Cool down events count: {CoolDownEventsCount}
* Inserted items count record (Top 20): {string.Join(", ", InsertItemsCountRecord.Take(20))}
* Current cooldown milliseconds: {_cooldownMilliseconds}
* Remaining write tasks count: {_tasksQueue.PendingTasksCount}

Underlying object bucket statistics:
{innerBucket.OutputStatistics().AppendTabsEachLineHead()}
";
    }

    public bool IsCold => CoolDownTimingTask?.IsCompleted ?? true;
    public bool IsHot => !IsCold;

    public int BufferedItemsCount
    {
        get
        {
            lock (_bufferWriteLock)
            {
                return _buffer.Count;
            }
        }
    }
    
    public Task? CoolDownTimingTask;

    public void AddBuffered(params T[] objs)
    {
        // Only single thread. Or multiple threads may believe this is Code at the same time.
        lock (_bufferWriteLock)
        {
            // In hot status, we couldn't add the data directly. Add to the buffer. Wait for cooldown to flush the buffer.
            if (IsHot)
            {
                Interlocked.Increment(ref RequestHotWriteCount);
                if (_buffer.Count + objs.Length >= maxBufferedItemsCount)
                {
                    // If buffer is full, wait until the buffer is cleared.
                    WaitCurrentCoolDownTaskAsync().Wait();
                }

                // In hot status, we couldn't add the data directly. Add to the buffer. Wait for cooldown to flush the buffer.
                foreach (var obj in objs)
                {
                    _buffer.Enqueue(obj);
                }
            }
            // Is cold status, directly queue add then start cooldown
            else
            {
                Interlocked.Increment(ref RequestColdWriteCount);
                // Add and start cooldown
                QueueAddBulk(objs);
                StartCooldown();
            }
        }
    }
    
    // This method should never execute in parallel!!!
    private void StartCooldown()
    {
        Interlocked.Increment(ref CoolDownEventsCount);
        // Now this instance is hot. Wait for cooldown to flush the buffer.
        CoolDownTimingTask = Task.Run(async () =>
        {
            await Task.Delay(_cooldownMilliseconds);

            // Consider the high frequency of writes, the queue may not be able to handle it.
            // According to the remaining tasks in the queue, increase the next cooldown time.
            // But not more than 16 times the initial cooldown time.
            var updatedCooldownMilliseconds = (_tasksQueue.PendingTasksCount + 1) * _initialCooldownMilliseconds;
            _cooldownMilliseconds = Math.Min(updatedCooldownMilliseconds, maxCooldownMilliseconds);

            lock (_bufferWriteLock)
            {
                if (!_buffer.IsEmpty)
                {
                    // Add and restart cooldown
                    QueueAddBulk(_buffer.ToArray());
                    // Clear the buffer
                    _buffer.Clear();
                    StartCooldown();
                }
                else
                {
                    _cooldownMilliseconds = _initialCooldownMilliseconds;
                }
            }
        });
    }

    private void QueueAddBulk(T[] objs)
    {
        Interlocked.Increment(ref QueuedWriteCount);
        _tasksQueue.QueueNew(() =>
        {
            // Statistics
            Interlocked.Increment(ref ActualWriteCount);
            InsertItemsCountRecord.Add(objs.Length);
            
            // Add to the underlying bucket
            try
            {
                innerBucket.AddBulk(objs);
            }
            catch (Exception e)
            {
                // We couldn't throw the exception here. Because the task queue will be stopped.
                Console.WriteLine(e);
            }
        });
    }

    /// <summary>
    /// If you inserted some data, then you call the `Sync` method, it will return a task, that when all the data is written to the disk, it will be completed.
    ///
    /// This method is used to ensure the data we inserted before called this method is written to the disk.
    /// </summary>
    public async Task SyncAsync()
    {
        await WaitCurrentCoolDownTaskAsync();
        await WaitWriteCompleteAsync();
    }
    
    /// <summary>
    /// If you inserted some data, then you call the `WaitUntilCoolAsync` method, it will return a task, that when all the data is cleared from buffer and started writing to the disk, it will be completed.
    ///
    /// This method is used to ensure the data is started writing to the disk.
    /// </summary>
    /// <returns></returns>
    public Task WaitCurrentCoolDownTaskAsync()
    {
        if (IsHot)
        {
            return (CoolDownTimingTask ?? Task.CompletedTask);
        }
        return Task.CompletedTask;
    }
    
    /// <summary>
    /// When the data started writing to the disk, you can call this method to wait until the writing is completed.
    ///
    /// This method is used to ensure the datta started writing was ultimately written to the disk. 
    /// </summary>
    /// <returns></returns>
    public Task WaitWriteCompleteAsync()
    {
        return _tasksQueue.Engine;
    }
}