using Aiursoft.ArrayDb.Partitions;
using Aiursoft.ArrayDb.Tests.Base.Models;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;
[TestClass]
public class PartitionedObjectBucketsTests
{
    private readonly string _testPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

    [TestInitialize]
    public void Init()
    {
        // Reset test path
        if (Directory.Exists(_testPath))
        {
            Directory.Delete(_testPath, true);
        }
        Directory.CreateDirectory(_testPath);
    }

    [TestCleanup]
    public void Clean()
    {
        // Clean test path
        if (Directory.Exists(_testPath))
        {
            Directory.Delete(_testPath, true);
        }
    }

    [TestMethod]
    public async Task TestAddPartitionedReboot()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 0; i < 100; i++)
        {
            var sample = new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 10).ToString(),
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();

        var partitionedService2 = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.ReadAll();
        Assert.AreEqual(10, partitionedService2.PartitionsCount, "There should be 10 partitions.");
        Assert.AreEqual(100, results.Length, "There should be 100 total results.");
        foreach (var result in results)
        {
            Assert.AreEqual(result.PartitionId, result.ThreadId, $"PartitionId should match ThreadId for result with Id {result.Id}.");
            Assert.AreEqual(result.PartitionId, (result.Id % 10).ToString(), $"PartitionId should match calculated partition key for result with Id {result.Id}.");
        }
    }

    [TestMethod]
    public async Task TestCountPartitioned()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 0; i < 100; i++)
        {
            var sample = new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 10).ToString(),
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();

        var partitionedService2 = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var totalCount = partitionedService2.Count();
        Assert.AreEqual(100, totalCount, "Total item count should be 100.");
        var countByPartition = partitionedService2.Count("5");
        Assert.AreEqual(10, countByPartition, "Partition '5' should contain 10 items.");
    }

    [TestMethod]
    public async Task TestAsEnumerablePartitioned()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 0; i < 200; i++)
        {
            var sample = new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 10).ToString(),
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();

        var partitionedService2 = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.AsEnumerable("5", bufferedReadPageSize: 13);
        var resultsArray = results.ToArray();
        Assert.AreEqual(20, resultsArray.Length, "Partition '5' should contain 20 items.");
        foreach (var result in resultsArray)
        {
            Assert.AreEqual("5", result.PartitionId, "PartitionId should be '5'.");
            Assert.AreEqual("5", (result.Id % 10).ToString(), "PartitionId should match calculated partition key.");
        }

        var resultsR = partitionedService2.AsReverseEnumerable("5", bufferedReadPageSize: 13);
        var resultsArrayR = resultsR.ToArray();
        Assert.AreEqual(20, resultsArrayR.Length, "Partition '5' should contain 20 items.");
        foreach (var result in resultsArrayR)
        {
            Assert.AreEqual("5", result.PartitionId, "PartitionId should be '5'.");
            Assert.AreEqual("5", (result.Id % 10).ToString(), "PartitionId should match calculated partition key.");
        }
    }

    [TestMethod]
    public async Task TestAsReverseEnumerablePartitioned()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 1; i <= 5; i++)
        {
            var sample = new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = "2333",
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();

        var partitionedService2 = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.AsReverseEnumerable("2333", bufferedReadPageSize: 2);

        // results should be: 5, 4, 3, 2, 1, 0
        var resultsArray = results.ToArray();

        Assert.AreEqual(5, resultsArray.Length, "Partition should contain 5 items.");
        Assert.AreEqual(5, resultsArray[0].Id, "First item should be 49.");
        Assert.AreEqual(4, resultsArray[1].Id, "Second item should be 48.");
        Assert.AreEqual(3, resultsArray[2].Id, "Third item should be 47.");
        Assert.AreEqual(2, resultsArray[3].Id, "Fourth item should be 46.");
        Assert.AreEqual(1, resultsArray[4].Id, "Fifth item should be 45.");
    }

    [TestMethod]
    public async Task TestDataWithDefaultPartitionKey()
    {
        var partitionedService = new PartitionedObjectBucket<DataWithDefaultPartition, int>("my-db2", _testPath);
        for (var i = 0; i < 100; i++)
        {
            var sample = new DataWithDefaultPartition
            {
                Id = i,
                PartitionId = i % 10,
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();

        var partitionedService2 = new PartitionedObjectBucket<DataWithDefaultPartition, int>("my-db2", _testPath);
        var results = partitionedService2.AsEnumerable(5).ToArray();
        Assert.AreEqual(10, results.Length, "Partition '5' should contain 10 items.");
        foreach (var result in results)
        {
            Assert.AreEqual(5, result.PartitionId, "PartitionId should be 5.");
            Assert.AreEqual(5, result.Id % 10, "Id modulo 10 should equal PartitionId.");
        }

        var resultsR = partitionedService2.AsReverseEnumerable(5).ToArray();
        Assert.AreEqual(10, resultsR.Length, "Partition '5' should contain 10 items.");
        foreach (var result in resultsR)
        {
            Assert.AreEqual(5, result.PartitionId, "PartitionId should be 5.");
            Assert.AreEqual(5, result.Id % 10, "Id modulo 10 should equal PartitionId.");
        }
    }

    [TestMethod]
    public async Task TestDeletePartitionAndReboot()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 0; i < 100; i++)
        {
            var sample = new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 10).ToString(),
                Message = $"Hello, World! 你好世界 {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();
        await partitionedService.DeletePartitionAsync("5");

        var partitionedService2 = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.ReadAll();
        Assert.AreEqual(9, partitionedService2.PartitionsCount, "There should be 9 partitions after deleting one.");
        Assert.AreEqual(90, results.Length, "There should be 90 items after deleting the partition.");
    }

    [TestMethod]
    public async Task TestSyncAcrossPartitions()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 0; i < 50; i++)
        {
            var sample = new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 5).ToString(),
                Message = $"Sync Test {i}"
            };
            partitionedService.Add(sample);
        }
        await partitionedService.SyncAsync();

        var partitionedService2 = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        Assert.AreEqual(5, partitionedService2.PartitionsCount, "There should be 5 partitions after syncing.");
        Assert.AreEqual(50, partitionedService2.Count(), "Total item count should be 50.");
    }

    [TestMethod]
    public void TestReadNonExistentPartition()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
        {
            _ = partitionedService.Read("nonexistent", 0);
        }, "Attempting to read from a non-existent partition should throw an exception.");
    }

    [TestMethod]
    public async Task TestReadExistentPartitionByIndex()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        partitionedService.Add(new DataCanBePartitionedByString
        {
            Id = 1,
            ThreadId = "partition1",
            Message = "Sample 1"
        });
        await partitionedService.SyncAsync();
        var result = partitionedService.Read("partition1", 0);
        Assert.AreEqual("partition1", result.PartitionId, "PartitionId should match the partition key.");
        Assert.AreEqual("Sample 1", result.Message, "Message should match the original value.");
    }

    [TestMethod]
    public async Task TestDeleteNonExistentPartition()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(async () =>
        {
            await partitionedService.DeletePartitionAsync("nonexistent");
        }, "Attempting to delete a non-existent partition should throw an InvalidOperationException.");
    }

    [TestMethod]
    public void TestGetPartitionByIdCreatesPartitionIfNotExists()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var partition = partitionedService.GetPartitionById("newPartition");
        Assert.IsNotNull(partition, "A new partition should be created if it does not exist.");
        Assert.AreEqual(1, partitionedService.PartitionsCount, "Partitions count should be 1 after creating a new partition.");
    }

    [TestMethod]
    public async Task TestAddObjectsAcrossPartitions()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);

        var samples = new List<DataCanBePartitionedByString>();
        for (var i = 0; i < 20; i++)
        {
            samples.Add(new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 4).ToString(),
                Message = $"Sample {i}"
            });
        }

        partitionedService.Add(samples.ToArray());
        await partitionedService.SyncAsync();

        Assert.AreEqual(4, partitionedService.PartitionsCount, "There should be 4 partitions after adding objects.");
        foreach (var i in Enumerable.Range(0, 4))
        {
            Assert.AreEqual(5, partitionedService.Count(i.ToString()), $"Partition {i} should contain 5 items.");
        }
    }

    [TestMethod]
    public async Task TestReadBulkWithPartialAndOutOfRange()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);

        for (var i = 0; i < 10; i++)
        {
            partitionedService.Add(new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = "partition1",
                Message = $"Sample {i}"
            });
        }
        await partitionedService.SyncAsync();

        var results = partitionedService.ReadBulk("partition1", 0, 5);
        Assert.AreEqual(5, results.Length, "Should read 5 items.");

        try
        {
            _ = partitionedService.ReadBulk("partition1", 8, 5);
            Assert.Fail("Reading beyond the available range should throw an exception.");
        }
        catch (ArgumentOutOfRangeException)
        {
            // Expected exception
        }
    }

    [TestMethod]
    public void TestCountOnEmptyPartition()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var count = partitionedService.Count("emptyPartition");
        Assert.AreEqual(0, count, "Count for a non-existent or empty partition should be 0.");
    }

    [TestMethod]
    public async Task TestConcurrentSyncAsync()
    {
        var partitionedService = new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        for (var i = 0; i < 50; i++)
        {
            partitionedService.Add(new DataCanBePartitionedByString
            {
                Id = i,
                ThreadId = (i % 5).ToString(),
                Message = $"Concurrent Test {i}"
            });
        }

        var syncTasks = new[]
        {
            partitionedService.SyncAsync(),
            partitionedService.SyncAsync(),
            partitionedService.SyncAsync()
        };

        await Task.WhenAll(syncTasks);
        Assert.AreEqual(5, partitionedService.PartitionsCount, "There should be 5 partitions after concurrent sync operations.");
    }
}
