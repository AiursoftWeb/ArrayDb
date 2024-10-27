namespace Aiursoft.ArrayDb.Engine;

public class BufferedObjectBuckets<T>(
    ObjectBuckets<T> innerBucket,
    int bufferCapacity = 65536,
    // Don't set this too small. Because only write large data in bulk can be efficient.
    // Don't set this too large. Because it will cause huge latency for the write operation.
    int cooldownMilliseconds = 1000)
    where T : new()
{
    private readonly TasksQueue _tasksQueue = new();
    private readonly List<T> _buffer = new(bufferCapacity);
    private readonly object _bufferWriteLock = new();

    // Initial status: 65536 available slots
    private readonly SemaphoreSlim _bufferSemaphore = new(bufferCapacity, bufferCapacity);
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
    
    public int AvailableSlots => _bufferSemaphore.CurrentCount;

    public Task? CoolDownTimingTask;

    public async Task AddAsync(T obj)
    {
        // Wait until there's at least one slot available
        // Available slots -= 1
        await _bufferSemaphore.WaitAsync();
        
        // Is cold status, directly queue add then start cooldown
        if (IsCold)
        {
            // Add and start cooldown
            QueueAddBulk([obj]);
            StartCooldown();

            // Available slots += 1
            _bufferSemaphore.Release(1);
        }
        // In hot status, we couldn't add the data directly. Add to the buffer. Wait for cooldown to flush the buffer.
        else
        {
            lock (_bufferWriteLock)
            {
                // In hot status, we couldn't add the data directly. Add to the buffer. Wait for cooldown to flush the buffer.
                _buffer.Add(obj);
            }
        }
    }
    
    private void StartCooldown()
    {
        // Now this instance is hot. Wait for cooldown to flush the buffer.
        CoolDownTimingTask = Task.Run(async () =>
        {
            await Task.Delay(cooldownMilliseconds);

            lock (_bufferWriteLock)
            {
                if (_buffer.Count != 0)
                {
                    var bufferItemsCount = _buffer.Count;
                    // Add and restart cooldown
                    QueueAddBulk(_buffer.ToArray());
                    StartCooldown();
                    
                    // Clear the buffer
                    _buffer.Clear();
                    _bufferSemaphore.Release(bufferItemsCount);
                }
            }
        });
    }

    private void QueueAddBulk(T[] objs)
    {
        _tasksQueue.QueueNew(() => Task.Run(() => innerBucket.AddBulk(objs)));
    }

    /// <summary>
    /// If you inserted some data, then you call the `Sync` method, it will return a task, that when all the data is written to the disk, it will be completed.
    /// </summary>
    public async Task Sync()
    {
        await WaitUntilCoolAsync();
        await WaitUntilWriteCompleteAsync();
    }
    
    public Task WaitUntilCoolAsync()
    {
        if (IsHot)
        {
            return (CoolDownTimingTask ?? Task.CompletedTask);
        }
        return Task.CompletedTask;
    }
    
    public Task WaitUntilWriteCompleteAsync()
    {
        return _tasksQueue.Engine;
    }
}