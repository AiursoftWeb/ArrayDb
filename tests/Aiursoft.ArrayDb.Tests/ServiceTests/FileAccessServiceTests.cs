using System.Diagnostics.CodeAnalysis;
using Aiursoft.ArrayDb.FilePersists.Services;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.ServiceTests;

[TestClass]
[DoNotParallelize]
public class FileAccessServiceTests : ArrayDbTestBase
{
    private const long InitialSize = 1024 * 1024; // 1 MB

    [NotNull]
    // ReSharper disable once RedundantDefaultMemberInitializer
    private FileAccessService? _service = null!;

    [TestInitialize]
    public void SetUp()
    {
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }

        _service = new FileAccessService(TestFilePath, InitialSize);
    }

    [TestCleanup]
    public void TearDown()
    {
        _service.Dispose();
        if (File.Exists(TestFilePath))
        {
            File.Delete(TestFilePath);
        }
    }

    [TestMethod]
    public void TestWriteAndReadBasic()
    {
        var data = "Hello, ArrayDb!"u8.ToArray();

        _service.WriteInFile(0, data);
        var result = _service.ReadInFile(0, data.Length);

        CollectionAssert.AreEqual(data, result);
        Assert.AreEqual(1, _service.SeekWriteCount);
        Assert.AreEqual(1, _service.SeekReadCount);
    }

    [TestMethod]
    public void TestWriteAndReadAtOffset()
    {
        var data1 = "First block"u8.ToArray();
        var data2 = "Second block"u8.ToArray();

        _service.WriteInFile(0, data1);
        _service.WriteInFile(data1.Length, data2);

        var read1 = _service.ReadInFile(0, data1.Length);
        var read2 = _service.ReadInFile(data1.Length, data2.Length);

        CollectionAssert.AreEqual(data1, read1);
        CollectionAssert.AreEqual(data2, read2);
    }

    [TestMethod]
    public void TestWriteTriggersFileExpansion()
    {
        Assert.AreEqual(0, _service.ExpandSizeCount);

        // Write data that extends beyond the initial file size
        var offset = InitialSize - 10;
        var largeData = new byte[100];
        new Random(42).NextBytes(largeData);

        _service.WriteInFile(offset, largeData);

        Assert.IsTrue(_service.ExpandSizeCount >= 1);
        var result = _service.ReadInFile(offset, largeData.Length);
        CollectionAssert.AreEqual(largeData, result);
    }

    [TestMethod]
    public void TestMultipleExpansions()
    {
        // First expansion
        var offset1 = InitialSize + 100;
        var data1 = new byte[50];
        new Random(42).NextBytes(data1);
        _service.WriteInFile(offset1, data1);

        // Second expansion further out
        var offset2 = InitialSize * 3 + 200;
        var data2 = new byte[75];
        new Random(99).NextBytes(data2);
        _service.WriteInFile(offset2, data2);

        Assert.IsTrue(_service.ExpandSizeCount >= 2);

        var result1 = _service.ReadInFile(offset1, data1.Length);
        var result2 = _service.ReadInFile(offset2, data2.Length);

        CollectionAssert.AreEqual(data1, result1);
        CollectionAssert.AreEqual(data2, result2);
    }

    [TestMethod]
    public void TestDeleteAsyncRemovesFile()
    {
        _service.WriteInFile(0, "test"u8.ToArray());
        Assert.IsTrue(File.Exists(TestFilePath));

        _service.DeleteAsync().GetAwaiter().GetResult();

        Assert.IsFalse(File.Exists(TestFilePath));
    }

    [TestMethod]
    public void TestDisposeClosesHandle()
    {
        _service.WriteInFile(0, "test"u8.ToArray());
        _service.Dispose();

        // After dispose, further operations on the file via RandomAccess would fail.
        // The file itself should still exist since Dispose only closes the handle.
        Assert.IsTrue(File.Exists(TestFilePath));
    }

    [TestMethod]
    public void TestConcurrentWrites()
    {
        const int threadCount = 8;
        const int writesPerThread = 100;
        var barrier = new Barrier(threadCount);
        var tasks = new Task[threadCount];

        for (var t = 0; t < threadCount; t++)
        {
            var threadIndex = t;
            tasks[t] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                for (var i = 0; i < writesPerThread; i++)
                {
                    var offset = threadIndex * writesPerThread * 64L + i * 64L;
                    var data = new byte[64];
                    data[0] = (byte)threadIndex;
                    data[63] = (byte)i;
                    _service.WriteInFile(offset, data);
                }
            });
        }

        Task.WaitAll(tasks);

        // Verify all writes persisted correctly
        for (var t = 0; t < threadCount; t++)
        {
            for (var i = 0; i < writesPerThread; i++)
            {
                var offset = t * writesPerThread * 64L + i * 64L;
                var result = _service.ReadInFile(offset, 64);
                Assert.AreEqual((byte)t, result[0], $"Thread marker mismatch at thread={t}, i={i}");
                Assert.AreEqual((byte)i, result[63], $"Index marker mismatch at thread={t}, i={i}");
            }
        }

        Assert.AreEqual(threadCount * writesPerThread, _service.SeekWriteCount);
        Assert.AreEqual(threadCount * writesPerThread, _service.SeekReadCount);
    }

    [TestMethod]
    public void TestConcurrentReadsAndWrites()
    {
        // Pre-populate data
        const int recordSize = 32;
        const int recordCount = 100;
        var expected = new byte[recordCount][];
        for (var i = 0; i < recordCount; i++)
        {
            expected[i] = new byte[recordSize];
            new Random(i).NextBytes(expected[i]);
            _service.WriteInFile(i * recordSize, expected[i]);
        }

        var writeCountBefore = _service.SeekWriteCount;

        const int readerCount = 4;
        const int writerCount = 4;
        var barrier = new Barrier(readerCount + writerCount);
        var tasks = new List<Task>();

        // Readers verify existing data
        for (var r = 0; r < readerCount; r++)
        {
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                for (var pass = 0; pass < 10; pass++)
                {
                    for (var i = 0; i < recordCount; i++)
                    {
                        var result = _service.ReadInFile(i * recordSize, recordSize);
                        CollectionAssert.AreEqual(expected[i], result, $"Reader mismatch at record {i}");
                    }
                }
            }));
        }

        // Writers write new data beyond existing records
        for (var w = 0; w < writerCount; w++)
        {
            var writerId = w;
            tasks.Add(Task.Run(() =>
            {
                barrier.SignalAndWait();
                for (var i = 0; i < 50; i++)
                {
                    var offset = (recordCount + writerId * 50 + i) * recordSize;
                    var data = new byte[recordSize];
                    data[0] = (byte)writerId;
                    _service.WriteInFile(offset, data);
                }
            }));
        }

        Task.WaitAll(tasks.ToArray());

        Assert.IsTrue(_service.SeekWriteCount > writeCountBefore);
    }

    [TestMethod]
    public void TestResetStatistics()
    {
        _service.WriteInFile(0, "test"u8.ToArray());
        _service.ReadInFile(0, 4);

        Assert.AreEqual(1, _service.SeekWriteCount);
        Assert.AreEqual(1, _service.SeekReadCount);

        _service.ResetAllStatistics();

        Assert.AreEqual(0, _service.SeekWriteCount);
        Assert.AreEqual(0, _service.SeekReadCount);
    }
}
