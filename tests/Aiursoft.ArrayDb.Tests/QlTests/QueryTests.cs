using System.Text;
using Aiursoft.ArrayDb.ArrayQl;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.QlTests;

[TestClass]
public class QueryTests : ArrayDbTestBase
{
    [TestMethod]
    public void TestQuery()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "MyNumber1", BucketItemPropertyType.Int32 },
                { "MyString1", BucketItemPropertyType.String },
                { "MyNumber2", BucketItemPropertyType.Int32 },
                { "MyBoolean1", BucketItemPropertyType.Boolean },
                { "MyString2", BucketItemPropertyType.String },
                { "MyFixedByteArray", BucketItemPropertyType.FixedSizeByteArray }
            },
            FixedByteArrayLengths = new Dictionary<string, int>
            {
                { "MyFixedByteArray", 30 }
            }
        };
        var persistService = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        for (var i = 0; i < 100; i++)
        {
            persistService.Add(new BucketItem
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
                    },
                    {
                        "MyNumber2",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Int32,
                            Value = i * 10
                        }
                    },
                    {
                        "MyBoolean1",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.Boolean,
                            Value = i % 2 == 0
                        }
                    },
                    {
                        "MyString2",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.String,
                            Value = $"This is another longer string. {i}"
                        }
                    },
                    {
                        "MyFixedByteArray",
                        new BucketItemPropertyValue
                        {
                            Type = BucketItemPropertyType.FixedSizeByteArray,
                            Value = Encoding.UTF8.GetBytes($"FixedByteArray {i}")
                        }
                    }
                }
            });
        }

        // Create parser
        var parser = new ArrayQlParser();

        // Execute a query
        var results = parser.Run("""
                                 source
                                 .Where(c => c.MyNumber1 % 2 == 0)
                                 """, persistService);

        Assert.AreEqual(50, results.Count());

        // Execute a query
        var resultsCount = parser.Run("""
                                 source
                                 .Where(c => c.MyNumber1 % 2 == 0)
                                 .OrderBy(c => c.MyString1)
                                 .Count()
                                 """, persistService);

        Assert.AreEqual(50, resultsCount.First() as int?);
    }
}

