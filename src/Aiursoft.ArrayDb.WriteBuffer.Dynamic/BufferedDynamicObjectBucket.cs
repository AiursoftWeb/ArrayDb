using System.Collections.Concurrent;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.WriteBuffer.Core;

namespace Aiursoft.ArrayDb.WriteBuffer.Dynamic;

public class BufferedDynamicObjectBucket(
    DynamicObjectBucket innerBucket,
    int maxSleepMilliSecondsWhenCold = Consts.Consts.MaxSleepMilliSecondsWhenCold,
    int stopSleepingWhenWriteBufferItemsMoreThan = Consts.Consts.WriteBufferStopSleepingWhenWriteBufferItemsMoreThan)
    : IDynamicObjectBucket
{
    private Task _engine = Task.CompletedTask;
    private Task _coolDownEngine = Task.CompletedTask;
    private readonly ReaderWriterLockSlim _bufferLock = new();
    private readonly object _bufferWriteSwapLock = new();
    private readonly object _engineStatusSwitchLock = new();
    private ConcurrentQueue<BucketItem> _activeBuffer = new();
    private ConcurrentQueue<BucketItem> _secondaryBuffer = new();

    public int WriteTimesCount;
    public int WriteItemsCount;
    public int ActualWriteTimesCount;
    public int ActualWriteItemsCount;
    public int CoolDownEventsCount;

    public int Count
    {
        get
        {
            _bufferLock.EnterReadLock();
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

    public void Add(params BucketItem[] objs)
    {
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

        if (!IsCold)
        {
            return;
        }

        lock (_engineStatusSwitchLock)
        {
            if (!IsCold)
            {
                return;
            }

            _engine = Task.Run(WriteBuffered);
        }
    }

    public bool IsCold => _engine.IsCompleted && _coolDownEngine.IsCompleted;
    public bool IsHot => !IsCold;

    private void WriteBuffered()
    {
        _bufferLock.EnterWriteLock();
        try
        {
            ConcurrentQueue<BucketItem> bufferToPersist;
            lock (_bufferWriteSwapLock)
            {
                bufferToPersist = _activeBuffer;
                _activeBuffer = _secondaryBuffer;
                _secondaryBuffer = new ConcurrentQueue<BucketItem>();
            }

            var dataToWrite = bufferToPersist.ToArray();

            bufferToPersist.Clear();

            Interlocked.Add(ref ActualWriteItemsCount, dataToWrite.Length);
            Interlocked.Increment(ref ActualWriteTimesCount);

            innerBucket.Add(dataToWrite);
        }
        finally
        {
            _bufferLock.ExitWriteLock();
        }

        if (!_activeBuffer.IsEmpty)
        {
            _coolDownEngine = Task.Run(async () =>
            {
                var sleepTime = TimeExtensions.CalculateSleepTime(
                    maxSleepMilliSecondsWhenCold,
                    stopSleepingWhenWriteBufferItemsMoreThan,
                    _activeBuffer.Count);
                await Task.Delay(sleepTime);

                _engine = Task.Run(WriteBuffered);
            });
        }
        else
        {
            Interlocked.Increment(ref CoolDownEventsCount);
        }
    }

    public BucketItem Read(int index)
    {
        _bufferLock.EnterReadLock();
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

    public BucketItem[] ReadBulk(int indexFrom, int take)
    {
        _bufferLock.EnterReadLock();
        try
        {
            if (take <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(take), "读取数量必须大于0。");
            }

            var totalItemsAvailable = innerBucket.Count + _activeBuffer.Count;

            if (indexFrom < 0 || indexFrom >= totalItemsAvailable)
            {
                throw new ArgumentOutOfRangeException(nameof(indexFrom), "索引超出范围。");
            }

            if (indexFrom + take > totalItemsAvailable)
            {
                throw new ArgumentOutOfRangeException(nameof(take), "读取范围超出可用项。");
            }

            if (indexFrom < innerBucket.Count)
            {
                var itemsToTakeFromInnerBucket = Math.Min(take, innerBucket.Count - indexFrom);
                var fromInnerBucket = innerBucket.ReadBulk(indexFrom, itemsToTakeFromInnerBucket);
                var remainingItemsToTake = take - itemsToTakeFromInnerBucket;

                if (remainingItemsToTake <= 0)
                    return fromInnerBucket;

                var fromActiveBuffer = _activeBuffer.Take(remainingItemsToTake).ToArray();
                return fromInnerBucket.Concat(fromActiveBuffer).ToArray();
            }

            var bufferIndex = indexFrom - innerBucket.Count;
            return _activeBuffer.Skip(bufferIndex).Take(take).ToArray();
        }
        finally
        {
            _bufferLock.ExitReadLock();
        }
    }

    public async Task DeleteAsync()
    {
        await SyncAsync();
        await innerBucket.DeleteAsync();
        _activeBuffer.Clear();
        _secondaryBuffer.Clear();
    }

    public string OutputStatistics()
    {
        return $@"
Buffered dynamic object bucket statistics:

* Current instance status: {(IsHot ? "Hot" : "Cold")}
* Is cooling down: {!_coolDownEngine.IsCompleted}
* Is writing: {!_engine.IsCompleted}
* Buffered items count: {BufferedItemsCount}
* Write times count: {WriteTimesCount}
* Write items count: {WriteItemsCount}
* Actual write events count: {ActualWriteTimesCount}
* Actual write items count: {ActualWriteItemsCount}
* Cool down events count: {CoolDownEventsCount}

Underlying dynamic object bucket statistics:
{innerBucket.OutputStatistics().AppendTabsEachLineHead()}
";
    }

    public async Task SyncAsync()
    {
        await _engine;
        await _coolDownEngine;
        await _engine;
    }

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
}