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
    private Task _engine = Task.CompletedTask;
    private Task _coolDownEngine = Task.CompletedTask;
    
    /// <summary>
    /// This lock protects from swapping the active and secondary buffers at the same time.
    /// </summary>
    private readonly object _bufferWriteSwapLock = new();
    /// <summary>
    /// This lock protects from switching the engine status at the same time.
    /// </summary>
    private readonly object _engineStatusSwitchLock = new();

    /// <summary>
    /// This lock protects from persisting the active buffer to disk at the same time.
    ///
    /// When you found this is locked, then currently the engine is writing the active buffer to disk.
    ///
    /// When you have this lock accessed, then it means the active buffer can treated as a layer of cache.
    /// </summary>
    private readonly object _persistingActiveBufferToDiskLock = new();

    private ConcurrentQueue<T> _activeBuffer = new();
    private ConcurrentQueue<T> _secondaryBuffer = new();

    public ObjectBucket<T> InnerBucket => innerBucket;
    public bool IsCold => _engine.IsCompleted && _coolDownEngine.IsCompleted;
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
* Is cooling down: {!_coolDownEngine.IsCompleted}
* Is writing: {!_engine.IsCompleted}
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
        if (!IsCold)
        {
            // Most of the cases in a high-frequency environment, the engine is still running.
            return;
        }

        // Engine might be sleeping. Wake it up.
        lock (_engineStatusSwitchLock)
        {
            // Avoid multiple threads to wake up the engine at the same time.
            if (!IsCold)
            {
                return;
            }

            _engine = Task.Run(WriteBuffered);
        }
    }

    private void WriteBuffered() // We can ensure this method is only called by the engine and never be executed by multiple threads at the same time.
    {
        lock (_persistingActiveBufferToDiskLock)
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
        }

        // While we are writing, new data may be added to the buffer. If so, we need to write it too.
        if (_activeBuffer.Count > 0)
        {
            // Restart the engine to write the new added data.
            _coolDownEngine = Task.Run(async () =>
            {
                // Slow down a little bit to wait for more data to come.
                // If we persist too fast, and the buffer is almost empty, frequent write will cause data fragmentation.
                // If we persist too slow, and a lot of new data has been added to the buffer, and the engine wasted time in sleeping.
                // So the time to sleep is calculated by the number of items in the buffer.
                var sleepTime = CalculateSleepTime(
                    maxSleepMilliSecondsWhenCold,
                    stopSleepingWhenWriteBufferItemsMoreThan,
                    _activeBuffer.Count);
                await Task.Delay(sleepTime);
                
                // Wake up the engine to write the new added data.
                _engine = Task.Run(WriteBuffered);
            });
        }
        else
        {
            Interlocked.Increment(ref CoolDownEventsCount);
        }
    }

    public async Task SyncAsync()
    {
        if (!_engine.IsCompleted)
        {
            // Engine is working. However, the buffer may still have data that after this phase, the data is still not written.
            // Wait two rounds of engine to finish to ensure all data is written.
            await _engine;
            // Cool down engine will ensure restart the engine to write the remaining data.
            await _coolDownEngine;
            // Wait for the engine to finish.
            await _engine;
        }
        else
        {
            // The engine is not working. In this case, it might be in the cool down phase.
            // Cool down engine will ensure restart the engine to write the remaining data.
            // Then wait for the engine to finish.
            await _coolDownEngine;
            await _engine;
        }
    }

    private static int CalculateSleepTime(double maxSleepMilliSecondsWhenCold,
        double stopSleepingWhenWriteBufferItemsMoreThan, int writeBufferItemsCount)
    {
        if (stopSleepingWhenWriteBufferItemsMoreThan <= 0)
        {
            throw new ArgumentException("B must be a positive number.");
        }

        if (writeBufferItemsCount > stopSleepingWhenWriteBufferItemsMoreThan)
        {
            return 0;
        }

        var y = maxSleepMilliSecondsWhenCold * (1 - Math.Log(1 + writeBufferItemsCount) /
            Math.Log(1 + stopSleepingWhenWriteBufferItemsMoreThan));
        return (int)y;
    }
}