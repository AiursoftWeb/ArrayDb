using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.WriteBuffer.Core;

namespace Aiursoft.ArrayDb.WriteBuffer;

/// <summary>
/// Represents a class for managing buffered objects in buckets.
/// </summary>
/// <typeparam name="T">The type of objects to be stored in the buckets.</typeparam>
/// <param name="innerBucket">The underlying object bucket to store the objects.</param>
public class BufferedObjectBuckets<T>(
    IObjectBucket<T> innerBucket,
    int maxSleepMilliSecondsWhenCold = Consts.Consts.MaxSleepMilliSecondsWhenCold,
    int stopSleepingWhenWriteBufferItemsMoreThan = Consts.Consts.WriteBufferStopSleepingWhenWriteBufferItemsMoreThan)
    : IObjectBucket<T> where T : new()
{
    private Task _engine = Task.CompletedTask;
    private Task _coolDownEngine = Task.CompletedTask;
    private readonly ReaderWriterLockSlim _bufferLock = new();

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

    public bool IsCold => _engine.IsCompleted && _coolDownEngine.IsCompleted;
    public bool IsHot => !IsCold;

    public int Count
    {
        get
        {
            _bufferLock.EnterReadLock(); // Get the buffer in a thread-safe way.
            try
            {
                return innerBucket.Count + _activeBuffer.Count;
            }
            finally
            {
                _bufferLock.ExitReadLock();
            }
        }
    }

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

    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        WriteTimesCount = 0;
        WriteItemsCount = 0;
        ActualWriteTimesCount = 0;
        ActualWriteItemsCount = 0;
        CoolDownEventsCount = 0;
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

Underlying object bucket statistics:
{innerBucket.OutputStatistics().AppendTabsEachLineHead()}
";
    }

    public void Add(params T[] objs)
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

        if (objs.Length == 0)
        {
            return;
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
        _bufferLock.EnterWriteLock(); // Get the buffer in a thread-safe way.
        try
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

            // Process the buffer to persist
            innerBucket.Add(dataToWrite);
        }
        finally
        {
            _bufferLock.ExitWriteLock();
        }

        // While we are writing, new data may be added to the buffer. If so, we need to write it too.
        if (!_activeBuffer.IsEmpty)
        {
            // Restart the engine to write the new added data.
            // Before engine quits, it wakes up cool down engine to ensure the engine will be restarted.
            // Before cool down quits, it wakes up the engine to ensure the engine will be restarted.
            // So if one of the two tasks is running, the engine will be restarted. And this buffer is in a hot state.
            // In a hot state, you don't have to start the engine again.
            _coolDownEngine = Task.Run(async () =>
            {
                // Slow down a little bit to wait for more data to come.
                // If we persist too fast, and the buffer is almost empty, frequent write will cause data fragmentation.
                // If we persist too slow, and a lot of new data has been added to the buffer, and the engine wasted time in sleeping.
                // So the time to sleep is calculated by the number of items in the buffer.
                var sleepTime = TimeExtensions.CalculateSleepTime(
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
        // Case 1:
        // Engine is working. However, the buffer may still have data that after this phase, the data is still not written.
        // Wait two rounds of engine to finish to ensure all data is written.
        // Cool down engine will ensure restart the engine to write the remaining data.
        // Wait for the engine to finish.

        // Case 2:
        // The engine is not working. In this case, it might be in the cool down phase.
        // The first wait is just await a completed task.
        // Cool down engine will ensure restart the engine to write the remaining data.
        // Then wait for the engine to finish.
        await _engine;
        await _coolDownEngine;
        await _engine;
    }

    public T Read(int index)
    {
        _bufferLock.EnterReadLock(); // Get the buffer in a thread-safe way.
        try
        {
            if (index < innerBucket.Count)
            {
                return innerBucket.Read(index);
            }
            else if (index < innerBucket.Count + _activeBuffer.Count)
            {
                return _activeBuffer.ElementAt(index - innerBucket.Count);
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(index), "Index is out of range.");
            }
        }
        finally
        {
            _bufferLock.ExitReadLock();
        }
    }

    public T[] ReadBulk(int indexFrom, int readItemsCount)
    {
        _bufferLock.EnterReadLock(); // Get the buffer in a thread-safe way.
        try
        {
            if (readItemsCount < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(readItemsCount), "Read items count must be greater than zero.");
            }

            if (readItemsCount == 0)
            {
                return Array.Empty<T>();
            }

            // Total items available for reading
            var innerCount = innerBucket.Count;
            var activeBufferItems = _activeBuffer.ToArray(); // Create a snapshot to prevent inconsistency during read
            var totalItemsAvailable = innerCount + activeBufferItems.Length;

            if (indexFrom < 0 || indexFrom >= totalItemsAvailable)
            {
                throw new ArgumentOutOfRangeException(nameof(indexFrom), "Index is out of range.");
            }

            // Check if the read range is valid
            if (indexFrom + readItemsCount > totalItemsAvailable)
            {
                throw new ArgumentOutOfRangeException(nameof(readItemsCount), "Read range exceeds available items.");
            }

            var result = new T[readItemsCount];
            var resultIndex = 0;

            // Case 1: Reading from innerBucket
            if (indexFrom < innerCount)
            {
                var itemsFromInner = Math.Min(readItemsCount, innerCount - indexFrom);
                var innerItems = innerBucket.ReadBulk(indexFrom, itemsFromInner);
                Array.Copy(innerItems, 0, result, 0, innerItems.Length);
                resultIndex = innerItems.Length;
            }

            // Case 2: Reading from active buffer
            if (resultIndex < readItemsCount)
            {
                var bufferStartIndex = Math.Max(0, indexFrom - innerCount);
                var itemsFromBuffer = readItemsCount - resultIndex;
                Array.Copy(activeBufferItems, bufferStartIndex, result, resultIndex, itemsFromBuffer);
            }

            return result;
        }
        finally
        {
            _bufferLock.ExitReadLock();
        }
    }

    public async Task DeleteAsync()
    {
        await SyncAsync(); // Sync to make sure all data is written to disk. Or delete will fail.
        await innerBucket.DeleteAsync();
        _activeBuffer.Clear();
        _secondaryBuffer.Clear();
    }
}
