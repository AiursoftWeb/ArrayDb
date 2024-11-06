using Aiursoft.ArrayDb.Partitions;
using Aiursoft.ArrayDb.Tests.Base.Models;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        var partitionedService =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
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
        
        var partitionedService2 =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.ReadAll();
        Assert.AreEqual(10, partitionedService2.PartitionsCount);
        Assert.AreEqual(100, results.Length);
        foreach (var result in results)
        {
            Assert.AreEqual(result.PartitionId, result.ThreadId);
            Assert.AreEqual(result.PartitionId, (result.Id % 10).ToString());
        }
    }

    [TestMethod]
    public async Task TestCountPartitioned()
    {
        var partitionedService =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
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
        
        var partitionedService2 =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var totalCount = partitionedService2.Count();
        Assert.AreEqual(100, totalCount);
        var countByPartition = partitionedService2.Count("5");
        Assert.AreEqual(10, countByPartition);
    }
    
    [TestMethod]
    public async Task TaskAsEnumerablePartitioned()
    {
        var partitionedService =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
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
        
        var partitionedService2 =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.AsEnumerable("5", bufferedReadPageSize: 13);
        var resultsArray = results.ToArray();
        Assert.AreEqual(20, resultsArray.Length);
        for (var i = 0; i < 20; i++)
        {
            Assert.AreEqual("5", resultsArray[i].PartitionId);
            Assert.AreEqual("5", (resultsArray[i].Id % 10).ToString());
        }
    }

    [TestMethod]
    public async Task DataWithDefaultPartitionKeyName()
    {
        var partitionedService =
            new PartitionedObjectBucket<DataWithDefaultPartition, int>("my-db2", _testPath);
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
        var partitionedService2 =
            new PartitionedObjectBucket<DataWithDefaultPartition, int>("my-db2", _testPath);
        var results = partitionedService2.AsEnumerable(5).ToArray();
        for (var i = 0; i < 10; i++)
        {
            Assert.AreEqual(10, results.Length);
            for (var j = 0; j < 10; j++)
            {
                Assert.AreEqual(5, results[j].PartitionId);
                Assert.AreEqual(5, results[j].Id % 10);
            }
        }
    }

    [TestMethod]
    public async Task DeletePartitionAndRebootTest()
    {
        var partitionedService =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
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
        var partitionedService2 =
            new PartitionedObjectBucket<DataCanBePartitionedByString, string>("my-db2", _testPath);
        var results = partitionedService2.ReadAll();
        Assert.AreEqual(9, partitionedService2.PartitionsCount);
        Assert.AreEqual(90, results.Length);
    }
}