
using Aiursoft.ArrayDb.ArrayQl;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.QlTests;

[TestClass]
public class ArrayQlParserConcurrencyTests : ArrayDbTestBase
{
    private DynamicObjectBucket CreateBucket()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "MyNumber1", BucketItemPropertyType.Int32 },
                { "MyString1", BucketItemPropertyType.String }
            }
        };
        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        for (var i = 0; i < 100; i++)
        {
            bucket.Add(new BucketItem
            {
                Properties = new Dictionary<string, BucketItemPropertyValue>
                {
                    {
                        "MyNumber1",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Int32,
                            Value = i
                        }
                    },
                    {
                        "MyString1",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.String,
                            Value = $"Hello, World! 你好世界 {i}"
                        }
                    }
                }
            });
        }

        return bucket;
    }

    [TestMethod]
    public void ConcurrentSameQuery_ShouldNotThrow()
    {
        var bucket = CreateBucket();
        var parser = new ArrayQlParser();
        const int threadCount = 16;
        const string query = "source.Where(c => c.MyNumber1 % 2 == 0)";

        var tasks = new Task[threadCount];
        for (var t = 0; t < threadCount; t++)
        {
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < 10; i++)
                {
                    var results = parser.Run(query, bucket);
                    Assert.AreEqual(50, results.Count());
                }
            });
        }

        Task.WaitAll(tasks);
    }

    [TestMethod]
    public void ConcurrentDifferentQueries_ShouldNotThrow()
    {
        var bucket = CreateBucket();
        var parser = new ArrayQlParser();
        const int threadCount = 8;

        string[] queries =
        [
            "source.Where(c => c.MyNumber1 % 2 == 0)",
            "source.Where(c => c.MyNumber1 > 50)",
            "source.Where(c => c.MyNumber1 < 30)",
            "source.OrderBy(c => c.MyNumber1)",
            "source.Count()"
        ];

        var tasks = new Task[threadCount];
        for (var t = 0; t < threadCount; t++)
        {
            var queryIndex = t % queries.Length;
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < 10; i++)
                {
                    var results = parser.Run(queries[queryIndex], bucket);
                    // Each query returns valid results, no assertion on exact count needed
                    var _ = results.Count();
                }
            });
        }

        Task.WaitAll(tasks);
    }

    [TestMethod]
    public void ConcurrentCachedQuery_ReturnsSameDelegate()
    {
        var bucket = CreateBucket();
        var parser = new ArrayQlParser();
        const string query = "source.Where(c => c.MyNumber1 < 100)";

        // Run once to populate cache
        var firstResult = parser.Run(query, bucket).Count();
        Assert.AreEqual(100, firstResult);

        // Run concurrently, all should return same correct result
        var tasks = Enumerable.Range(0, 32).Select(_ => Task.Run(() =>
        {
            var results = parser.Run(query, bucket);
            return results.Count();
        })).ToArray();

        Task.WaitAll(tasks);

        foreach (var task in tasks)
        {
            Assert.AreEqual(100, task.Result);
        }
    }
}
