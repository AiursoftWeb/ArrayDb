using System.Diagnostics.CodeAnalysis;
using Aiursoft.ArrayDb.FilePersists;

namespace Aiursoft.ArrayDb.Engine;

public class BufferedObjectBuckets<T>(
    ObjectBuckets<T> innerBucket,
    // Don't set this too small. Because only write large data in bulk can be efficient.
    // Don't set this too large. Because it will cause huge latency for the write operation.
    int cooldownMilliseconds = 1000)
    where T : new()
{
    private readonly TasksQueue _tasksQueue = new();
    private readonly List<T> _buffer = [];
    private readonly object _bufferWriteLock = new();
    
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

* Request write events count: {RequestHotWriteCount + RequestColdWriteCount}
* Requested hot write events count: {RequestHotWriteCount}
* Requested cold write events count: {RequestColdWriteCount}
* Queued write events count: {QueuedWriteCount}
* Actual write events count: {ActualWriteCount}
* Cool down events count: {CoolDownEventsCount}
* Inserted items count record (Top 20): {string.Join(", ", InsertItemsCountRecord.Take(20))}

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

    public void AddBuffered(T obj)
    {
        // In hot status, we couldn't add the data directly. Add to the buffer. Wait for cooldown to flush the buffer.
        if (IsHot)
        {
            Interlocked.Increment(ref RequestHotWriteCount);
            lock (_bufferWriteLock)
            {
                // In hot status, we couldn't add the data directly. Add to the buffer. Wait for cooldown to flush the buffer.
                _buffer.Add(obj);
            }
        }
        // Is cold status, directly queue add then start cooldown
        else
        {
            Interlocked.Increment(ref RequestColdWriteCount);
            // Add and start cooldown
            QueueAddBulk([obj]);
            StartCooldown();
        }
    }
    
    private void StartCooldown()
    {
        Interlocked.Increment(ref CoolDownEventsCount);
        // Now this instance is hot. Wait for cooldown to flush the buffer.
        CoolDownTimingTask = Task.Run(async () =>
        {
            await Task.Delay(cooldownMilliseconds);

            lock (_bufferWriteLock)
            {
                if (_buffer.Count != 0)
                {
                    // Add and restart cooldown
                    QueueAddBulk(_buffer.ToArray());
                    StartCooldown();
                    
                    // Clear the buffer
                    _buffer.Clear();
                }
            }
        });
    }

    private void QueueAddBulk(T[] objs)
    {
        Interlocked.Increment(ref QueuedWriteCount);
        _tasksQueue.QueueNew(() =>
        {
            Interlocked.Increment(ref ActualWriteCount);
            InsertItemsCountRecord.Add(objs.Length);
            return Task.Run(() => innerBucket.AddBulk(objs));
        });
    }

    /// <summary>
    /// If you inserted some data, then you call the `Sync` method, it will return a task, that when all the data is written to the disk, it will be completed.
    ///
    /// This method is used to ensure the data we inserted before called this method is written to the disk.
    /// </summary>
    public async Task SyncAsync()
    {
        await WaitUntilCoolAsync();
        await WaitWriteCompleteAsync();
    }
    
    /// <summary>
    /// If you inserted some data, then you call the `WaitUntilCoolAsync` method, it will return a task, that when all the data is cleared from buffer and started writing to the disk, it will be completed.
    ///
    /// This method is used to ensure the data is started writing to the disk.
    /// </summary>
    /// <returns></returns>
    public Task WaitUntilCoolAsync()
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