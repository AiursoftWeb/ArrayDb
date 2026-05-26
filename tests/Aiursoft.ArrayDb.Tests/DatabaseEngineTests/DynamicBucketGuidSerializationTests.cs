using System.Runtime.InteropServices;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class DynamicBucketGuidSerializationTests : ArrayDbTestBase
{
    [TestMethod]
    public void GuidRoundTrip_SingleItem()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "GuidProp", BucketItemPropertyType.Guid }
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var testGuid = Guid.NewGuid();
        bucket.Add(new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "GuidProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Guid, Value = testGuid } }
            }
        });

        var readBack = bucket.Read(0);
        Assert.AreEqual(testGuid, (Guid)readBack.Properties["GuidProp"].Value!,
            "Guid should round-trip correctly.");
    }

    [TestMethod]
    public void GuidRoundTrip_BulkItems()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "GuidProp", BucketItemPropertyType.Guid }
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var guids = new Guid[1000];
        var items = new BucketItem[1000];
        for (var i = 0; i < 1000; i++)
        {
            guids[i] = Guid.NewGuid();
            items[i] = new BucketItem
            {
                Properties = new Dictionary<string, BucketItemPropertyValue>
                {
                    { "GuidProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Guid, Value = guids[i] } }
                }
            };
        }

        bucket.Add(items);

        // Read individual
        for (var i = 0; i < 1000; i++)
        {
            var readBack = bucket.Read(i);
            Assert.AreEqual(guids[i], (Guid)readBack.Properties["GuidProp"].Value!,
                $"Guid at index {i} should round-trip correctly (single read).");
        }

        // Read bulk
        var bulkRead = bucket.ReadBulk(0, 1000);
        for (var i = 0; i < 1000; i++)
        {
            Assert.AreEqual(guids[i], (Guid)bulkRead[i].Properties["GuidProp"].Value!,
                $"Guid at index {i} should round-trip correctly (bulk read).");
        }
    }

    [TestMethod]
    public void GuidRoundTrip_SpecialValues()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "GuidProp", BucketItemPropertyType.Guid }
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        var guids = new[]
        {
            Guid.Empty,
            new Guid("00000000-0000-0000-0000-000000000001"),
            new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            new Guid("01234567-89ab-cdef-0123-456789abcdef"),
            new Guid("fedcba98-7654-3210-fedc-ba9876543210")
        };

        for (var i = 0; i < guids.Length; i++)
        {
            bucket.Add(new BucketItem
            {
                Properties = new Dictionary<string, BucketItemPropertyValue>
                {
                    { "GuidProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Guid, Value = guids[i] } }
                }
            });

            var readBack = bucket.Read(i);
            Assert.AreEqual(guids[i], (Guid)readBack.Properties["GuidProp"].Value!,
                $"Special Guid at index {i} should round-trip correctly.");
        }
    }

    [TestMethod]
    public void GuidRoundTrip_NullValueTreatsAsEmpty()
    {
        var typeDefine = new BucketItemTypeDefinition
        {
            Properties = new Dictionary<string, BucketItemPropertyType>
            {
                { "GuidProp", BucketItemPropertyType.Guid }
            }
        };

        var bucket = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

        bucket.Add(new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                { "GuidProp", new BucketItemPropertyValue { Type = BucketItemPropertyType.Guid, Value = null } }
            }
        });

        var readBack = bucket.Read(0);
        Assert.AreEqual(Guid.Empty, (Guid)readBack.Properties["GuidProp"].Value!,
            "Null Guid value should be serialized as Guid.Empty and round-trip correctly.");
    }

    [TestMethod]
    public void GuidMemoryLayout_MatchesToByteArray()
    {
        // Verify that MemoryMarshal.Write produces the same bytes as Guid.ToByteArray()
        var testGuid = Guid.NewGuid();
        var guidBytes = testGuid.ToByteArray();
        var marshalBytes = new byte[16];
        MemoryMarshal.Write(marshalBytes.AsSpan(), in testGuid);

        CollectionAssert.AreEqual(guidBytes, marshalBytes,
            "MemoryMarshal.Write should produce the same byte layout as Guid.ToByteArray().");
    }
}
