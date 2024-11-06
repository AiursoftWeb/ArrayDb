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
public class BufferedObjectBuckets<T>(
    ObjectBucket<T> innerBucket,
    int maxSleepMilliSecondsWhenCold = Consts.Consts.MaxSleepMilliSecondsWhenCold,
    int stopSleepingWhenWriteBufferItemsMoreThan = Consts.Consts.WriteBufferStopSleepingWhenWriteBufferItemsMoreThan)
    where T : BucketEntity, new()
{
    private Task _engine  = Task.CompletedTask;
    /// <summary>
    /// This lock protects from swapping the active and secondary buffers at the same time.
    /// </summary>
    private readonly object _bufferWriteSwapLock = new();
    /// <summary>
    /// This lock protects from switching the engine status at the same time.
    /// </summary>
    private readonly object _engineStatusSwitchLock = new();
    private ConcurrentQueue<T> _activeBuffer = new();
    private ConcurrentQueue<T> _secondaryBuffer = new();
    
    public ObjectBucket<T> InnerBucket => innerBucket;
    public bool IsCold => _engine.IsCompleted;
    public bool IsHot => !IsCold;
    
    // Statistics
    public int WriteTimesCount;
    public int WriteItemsCount;
    public int ActualWriteTimesCount;
    public int ActualWriteItemsCount;
    public int CoolDownEventsCount;

    public int BufferedItemsCount
    {
      get
      {
          lock (_bufferWriteSwapLock)
          {
              return _activeBuffer.Count;
          }
      }  
    }
    
    public readonly List<int> InsertItemsCountRecord = [];
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        WriteTimesCount = 0;
        WriteItemsCount = 0;
        ActualWriteTimesCount = 0;
        ActualWriteItemsCount = 0;
        CoolDownEventsCount = 0;
        InsertItemsCountRecord.Clear();
    }
    
    public string OutputStatistics()
    {
        return $@"
Buffered object repository with item type {typeof(T).Name} statistics:

* Current instance status: {(IsHot ? "Hot" : "Cold")}
* Buffered items count: {BufferedItemsCount}
* Write times count: {WriteTimesCount}
* Write items count: {WriteItemsCount}
* Actual write events count: {ActualWriteTimesCount}
* Actual write items count: {ActualWriteItemsCount}
* Cool down events count: {CoolDownEventsCount}
* Inserted items count record (Top 20): {string.Join(", ", InsertItemsCountRecord.Take(20))}

Underlying object bucket statistics:
{innerBucket.OutputStatistics().AppendTabsEachLineHead()}
";
    }
    
    public void AddBuffered(params T[] objs)
    {
        // This method provides a way to add object without blocking the thread.
        // Just add to buffer, and wake the engine to write them. Engine works in another thread.
        
        // Update statistics
        Interlocked.Increment(ref WriteTimesCount);
        Interlocked.Add(ref WriteItemsCount, objs.Length);
        
        lock (_bufferWriteSwapLock)
        {
            foreach (var obj in objs)
            {
                _activeBuffer.Enqueue(obj);
            }
        }
        
        // Get the engine status in advanced to avoid lock contention.
        if (!_engine.IsCompleted)
        {
            // Most of the cases in a high-frequency environment, the engine is still running.
            return;
        }

        // Engine might be sleeping. Wake it up.
        lock (_engineStatusSwitchLock)
        {
            // Avoid multiple threads to wake up the engine at the same time.
            if (!_engine.IsCompleted)
            {
                return;
            }

            _engine = Task.Run(WriteBuffered);
        }
    }

    private async Task WriteBuffered()
    {
        ConcurrentQueue<T> bufferToPersist;
        lock (_bufferWriteSwapLock)
        {
            // Swap active and secondary buffers
            bufferToPersist = _activeBuffer;
            _activeBuffer = _secondaryBuffer;
            _secondaryBuffer = new ConcurrentQueue<T>();
        }
        
        var dataToWrite = bufferToPersist.ToArray();
        
        // Release the buffer to avoid memory leak.
        bufferToPersist.Clear();
        
        // Update statistics
        Interlocked.Add(ref ActualWriteItemsCount, dataToWrite.Length);
        Interlocked.Increment(ref ActualWriteTimesCount);
        InsertItemsCountRecord.Add(dataToWrite.Length);

        // Process the buffer to persist
        innerBucket.AddBulk(dataToWrite);

        // Slow down a little bit to wait for more data to come.
        // If we persist too fast, and the buffer is almost empty, frequent write will cause data fragmentation.
        // If we persist too slow, and a lot of new data has been added to the buffer, and the engine wasted time in sleeping.
        // So the time to sleep is calculated by the number of items in the buffer.
        var sleepTime = CalculateSleepTime(
            maxSleepMilliSecondsWhenCold,
            stopSleepingWhenWriteBufferItemsMoreThan,
            _activeBuffer.Count);
        await Task.Delay(sleepTime);
      
        // While we are writing, new data may be added to the buffer. If so, we need to write it too.
        if (_activeBuffer.Count > 0)
        {
            // Use recursion to keep working on cleaning the buffer. Until it's empty.
            await WriteBuffered();
        }
        else
        {
            Interlocked.Increment(ref CoolDownEventsCount);
        }
    }

    public async Task SyncAsync() // This method ensure all data called AddBuffered before will be persisted.
    {
        await _engine;
    }
    
    private static int CalculateSleepTime(double maxSleepMilliSecondsWhenCold, double stopSleepingWhenWriteBufferItemsMoreThan, int writeBufferItemsCount)
    {
        if (stopSleepingWhenWriteBufferItemsMoreThan <= 0)
        {
            throw new ArgumentException("B must be a positive number.");
        }

        if (writeBufferItemsCount > stopSleepingWhenWriteBufferItemsMoreThan)
        {
            return 0;
        }

        var y = maxSleepMilliSecondsWhenCold * (1 - Math.Log(1 + writeBufferItemsCount) / Math.Log(1 + stopSleepingWhenWriteBufferItemsMoreThan));
        return (int)y;
    }
}