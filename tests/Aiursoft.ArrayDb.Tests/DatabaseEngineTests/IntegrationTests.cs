using System.Diagnostics;
using System.Text;
using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.Tests.Base;
using Aiursoft.ArrayDb.Tests.Base.Models;

namespace Aiursoft.ArrayDb.Tests.DatabaseEngineTests;

[TestClass]
[DoNotParallelize]
public class IntegrationTests : ArrayDbTestBase
{
    [TestMethod]
    public void TestWriteAndRead()
    {
        var testStartTime = DateTime.UtcNow;
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        for (var i = 0; i < 1; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            persistService.Add(sample);
        }

        var testEndTime = DateTime.UtcNow;

        for (var i = 0; i < 1; i++)
        {
            var readSample = persistService.Read(i);
            Assert.AreEqual(i, readSample.MyNumber1, "The value of MyNumber1 should match the expected value.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1,
                "The value of MyString1 should match the expected value.");
            Assert.AreEqual(i * 10, readSample.MyNumber2, "The value of MyNumber2 should match the expected value.");
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1,
                "The value of MyBoolean1 should match the expected value.");
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2,
                "The value of MyString2 should match the expected value.");
            Assert.IsTrue(testStartTime < readSample.CreationTime,
                $"CreationTime should be greater than testStartTime. However, test time is {testStartTime} and creation time is {readSample.CreationTime}.");
            Assert.IsTrue(testEndTime > readSample.CreationTime, "CreationTime should be less than testEndTime.");
        }
    }

    [TestMethod]
    [Obsolete("This test covers the scenario of writing an empty string, which is slow but necessary.")]
    public void TestWriteAndReadEmptyString()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        var sample = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = string.Empty,
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null // All null strings will be converted to empty string.
        };
        persistService.Add(sample);
        var readSample = persistService.Read(0);
        Assert.AreEqual(1, readSample.MyNumber1, "The value of MyNumber1 should be 1.");
        Assert.AreEqual(string.Empty, readSample.MyString1, "The value of MyString1 should be an empty string.");
        Assert.AreEqual(2 * 10, readSample.MyNumber2, "The value of MyNumber2 should be 20.");
        Assert.AreEqual(3 % 2 == 0, readSample.MyBoolean1, "The value of MyBoolean1 should be true.");
        Assert.AreEqual(string.Empty, readSample.MyString2, "The value of MyString2 should be an empty string.");
    }

    [TestMethod]
    [Obsolete("This test covers the scenario of rebooting the service, which is slow but necessary.")]
    public void TestRebootService()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        var sample = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "我和我的祖国 Oh",
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null // All null strings will be converted to empty string.
        };
        var sample2 = new SampleData
        {
            MyNumber1 = 1,
            MyString1 = "My country and I 啊",
            MyNumber2 = 2 * 10,
            MyBoolean1 = 3 % 2 == 0,
            MyString2 = null // All null strings will be converted to empty string.
        };
        persistService.Add(sample);
        persistService.Add(sample2);

        var persistService2 = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        Assert.AreEqual(2, persistService2.SpaceProvisionedItemsCount,
            "The number of space-provisioned items should be 2.");
        Assert.AreEqual(2, persistService2.ArchivedItemsCount, "The number of archived items should be 2.");
        //var offset = persistService2.StringRepository.FileEndOffset;
        //Assert.AreEqual(49, offset, "The file end offset should be 49.");

        var readSample = persistService2.Read(0);
        Assert.AreEqual(1, readSample.MyNumber1, "The value of MyNumber1 should be 1.");
        Assert.AreEqual("我和我的祖国 Oh", readSample.MyString1, "The value of MyString1 should be '我和我的祖国 Oh'.");
        Assert.AreEqual(2 * 10, readSample.MyNumber2, "The value of MyNumber2 should be 20.");
        Assert.AreEqual(3 % 2 == 0, readSample.MyBoolean1, "The value of MyBoolean1 should be true.");
        Assert.AreEqual(string.Empty, readSample.MyString2, "The value of MyString2 should be an empty string.");

        var readSample2 = persistService2.Read(1);
        Assert.AreEqual(1, readSample2.MyNumber1, "The value of MyNumber1 should be 1.");
        Assert.AreEqual("My country and I 啊", readSample2.MyString1,
            "The value of MyString1 should be 'My country and I 啊'.");
        Assert.AreEqual(2 * 10, readSample2.MyNumber2, "The value of MyNumber2 should be 20.");
        Assert.AreEqual(3 % 2 == 0, readSample2.MyBoolean1, "The value of MyBoolean1 should be true.");
        Assert.AreEqual(string.Empty, readSample2.MyString2, "The value of MyString2 should be an empty string.");
    }

    [TestMethod]
    public void TestComplicatedSampleData()
    {
        var persistService = new ObjectBucket<ComplicatedSampleData>(TestFilePath, TestFilePathStrings);

        var sample = new ComplicatedSampleData
        {
            MyString1 = "Hello, World! 你好世界！",
            MyDateTime1 = DateTime.Now,
            MyLong1 = 1234567890,
            MyFloat1 = 123.456f,
            MyDouble1 = 123.456,
            MyTimeSpan1 = TimeSpan.FromDays(1),
            MyGuid1 = Guid.NewGuid()
        };
        persistService.Add(sample);

        var result = persistService.ReadBulk(0, 1);

        Assert.AreEqual(sample.MyString1, result[0].MyString1, "The string value should match the original value.");
        Assert.AreEqual(sample.MyDateTime1, result[0].MyDateTime1,
            "The DateTime value should match the original value.");
        Assert.AreEqual(sample.MyLong1, result[0].MyLong1, "The long value should match the original value.");
        Assert.AreEqual(sample.MyFloat1, result[0].MyFloat1, "The float value should match the original value.");
        Assert.AreEqual(sample.MyDouble1, result[0].MyDouble1, "The double value should match the original value.");
        Assert.AreEqual(sample.MyTimeSpan1, result[0].MyTimeSpan1,
            "The TimeSpan value should match the original value.");
        Assert.AreEqual(sample.MyGuid1, result[0].MyGuid1, "The Guid value should match the original value.");
    }

    [TestMethod]
    public void TestAddBulkAndReadBulk()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        var samples = new List<SampleData>();
        for (var i = 0; i < 2; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            samples.Add(sample);
        }

        var samplesArray = samples.ToArray();
        persistService.Add(samplesArray);

        var persistService2 = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var readSamples = persistService2.ReadBulk(0, 2);

        for (var i = 0; i < 2; i++)
        {
            var readSample = readSamples[i];
            Assert.AreEqual(i, readSample.MyNumber1, $"The value of MyNumber1 for index {i} should match.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1,
                $"The value of MyString1 for index {i} should match.");
            Assert.AreEqual(i * 10, readSample.MyNumber2, $"The value of MyNumber2 for index {i} should match.");
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1, $"The value of MyBoolean1 for index {i} should match.");
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2,
                $"The value of MyString2 for index {i} should match.");
        }
    }

    [TestMethod]
    [Obsolete("This test covers a mixed scenario of bulk and individual writes, which is slow but necessary.")]
    public void TestMixedBulkAndIndividualWrites()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        // Bulk write
        var samples = new List<SampleData>();
        for (var i = 0; i < 2; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            samples.Add(sample);
        }

        persistService.Add(samples.ToArray());

        // Individual writes
        for (var i = 2; i < 5; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            persistService.Add(sample);
        }

        // Read and verify
        var persistService2 = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var readSamples = persistService2.ReadBulk(0, 5);
        for (var i = 0; i < 5; i++)
        {
            var readSample = readSamples[i];
            Assert.AreEqual(i, readSample.MyNumber1, $"The value of MyNumber1 for index {i} should match.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1,
                $"The value of MyString1 for index {i} should match.");
            Assert.AreEqual(i * 10, readSample.MyNumber2, $"The value of MyNumber2 for index {i} should match.");
            Assert.AreEqual(i % 2 == 0, readSample.MyBoolean1, $"The value of MyBoolean1 for index {i} should match.");
            Assert.AreEqual($"This is another longer string. {i}", readSample.MyString2,
                $"The value of MyString2 for index {i} should match.");
        }

        Assert.AreEqual(5, persistService2.SpaceProvisionedItemsCount,
            "The number of space-provisioned items should be 5.");
        Assert.AreEqual(5, persistService2.ArchivedItemsCount, "The number of archived items should be 5.");
    }

    [TestMethod]
    public void TestReadOutOfRange()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        // Write samples
        var samples = new List<SampleData>();
        for (var i = 0; i < 2; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            samples.Add(sample);
        }

        persistService.Add(samples.ToArray());

        // Read out-of-range item
        try
        {
            _ = persistService.Read(2);
            Assert.Fail("An exception should be thrown when attempting to read out-of-range data.");
        }
        catch (ArgumentOutOfRangeException e)
        {
            Assert.AreEqual("Specified argument was out of the range of valid values. (Parameter 'index')", e.Message,
                "The exception message should match.");
        }

        // Bulk read with out-of-range values
        try
        {
            _ = persistService.ReadBulk(1, 2);
            Assert.Fail("An exception should be thrown when attempting to read bulk data out of range.");
        }
        catch (ArgumentOutOfRangeException e)
        {
            Assert.AreEqual("Specified argument was out of the range of valid values. (Parameter 'indexFrom')",
                e.Message, "The exception message should match.");
        }

        // Read valid item
        var readSample = persistService.Read(1);
        Assert.AreEqual(1, readSample.MyNumber1, "The value of MyNumber1 for index 1 should match.");
    }

    [TestMethod]
    public void TestMultipleThreadsAddBulk()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);

        var threads = new List<Thread>();
        for (var i = 0; i < 1000; i++)
        {
            var i1 = i;
            var thread = new Thread(() =>
            {
                var samples = new List<SampleData>();
                for (var j = 0; j < 1000; j++)
                {
                    var sample = new SampleData
                    {
                        MyNumber1 = i1 * 1000 + j,
                        MyString1 = $"Hello, World! 你好世界 {i1 * 1000 + j}",
                        MyNumber2 = j * 10,
                        MyBoolean1 = j % 2 == 0,
                        MyString2 = $"This is another longer string. {j}"
                    };
                    samples.Add(sample);
                }

                persistService.Add(samples.ToArray());
            });
            threads.Add(thread);
        }

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        var count = persistService.Count;
        Assert.AreEqual(1000000, count, "The count of items should be 1,000,000.");

        var readSamples = persistService
            .ReadBulk(0, 1000000)
            .OrderBy(t => t.MyNumber1)
            .ToArray();
        for (var i = 0; i < 1000000; i++)
        {
            var readSample = readSamples[i];
            Assert.AreEqual(i, readSample.MyNumber1,
                $"The value of MyNumber1 for index {i} should match. It's string is {readSample.MyString1}");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", readSample.MyString1,
                $"The value of MyString1 for index {i} should match.");
        }
    }

    [TestMethod]
    public async Task TestAsyncSaveItems()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var sampleDataItems = new List<SampleData>();
        for (var i = 0; i < 100000; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            sampleDataItems.Add(sample);
        }

        var addTask = Task.Run(() => persistService.Add(sampleDataItems.ToArray()));
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        while (persistService.SpaceProvisionedItemsCount == persistService.ArchivedItemsCount)
        {
            await Task.Delay(1);
        }

        Assert.AreEqual(100000, persistService.SpaceProvisionedItemsCount, "All items should be provisioned.");
        Assert.AreEqual(0, persistService.ArchivedItemsCount, "No items should be archived at this point.");

        await addTask;
        Assert.AreEqual(100000, persistService.SpaceProvisionedItemsCount, "All items should be provisioned.");
        Assert.AreEqual(100000, persistService.ArchivedItemsCount, "All items should be archived.");
    }

    [TestMethod]
    public void TestReadAsEnumerable()
    {
        var persistService = new ObjectBucket<SampleData>(TestFilePath, TestFilePathStrings);
        var sampleDataItems = new List<SampleData>();

        // Create 200 sample items
        for (var i = 0; i < 200; i++)
        {
            var sample = new SampleData
            {
                MyNumber1 = i,
                MyString1 = $"Hello, World! 你好世界 {i}",
                MyNumber2 = i * 10,
                MyBoolean1 = i % 2 == 0,
                MyString2 = $"This is another longer string. {i}"
            };
            sampleDataItems.Add(sample);
        }

        // Add bulk data to the persistent service
        persistService.Add(sampleDataItems.ToArray());

        // Read data as an enumerable with buffered read
        var results = persistService.AsEnumerable(bufferedReadPageSize: 128);
        var resultsArray = results.ToArray();

        // Verify the number of results
        Assert.HasCount(200, resultsArray, "The number of results should match the number of inserted samples.");

        // Verify each item matches the expected data
        for (var i = 0; i < 200; i++)
        {
            Assert.AreEqual(i, resultsArray[i].MyNumber1, $"The value of MyNumber1 for index {i} should match.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", resultsArray[i].MyString1,
                $"The value of MyString1 for index {i} should match.");
            Assert.AreEqual(i * 10, resultsArray[i].MyNumber2, $"The value of MyNumber2 for index {i} should match.");
            Assert.AreEqual(i % 2 == 0, resultsArray[i].MyBoolean1,
                $"The value of MyBoolean1 for index {i} should match.");
            Assert.AreEqual($"This is another longer string. {i}", resultsArray[i].MyString2,
                $"The value of MyString2 for index {i} should match.");
        }
    }

    [TestMethod]
    public void TestBytesData()
    {
        var persistService = new ObjectBucket<BytesData>(TestFilePath, TestFilePathStrings);
        for (var i = 0; i < 200; i++)
        {
            var sample = new BytesData
            {
                AdeId = i,
                BytesText = Encoding.UTF8.GetBytes($"Hello, World! 你好世界 {i}"),
                ZdexId = i * 10
            };
            persistService.Add(sample);
        }

        var newPersistService = new ObjectBucket<BytesData>(TestFilePath, TestFilePathStrings);
        for (var i = 0; i < 200; i++)
        {
            var readSample = newPersistService.Read(i);

            // Trim the ending zeros
            var bytes = readSample.BytesText.TrimEndZeros();
            Assert.AreEqual(i, readSample.AdeId, $"The value of AdeId for index {i} should match.");
            Assert.AreEqual($"Hello, World! 你好世界 {i}", Encoding.UTF8.GetString(bytes),
                $"The value of BytesText for index {i} should match.");
            Assert.AreEqual(i * 10, readSample.ZdexId, $"The value of ZdexId for index {i} should match.");
        }
    }

    [TestMethod]
    public void TestBytesDataTooLong()
    {
        var persistService = new ObjectBucket<BytesData>(TestFilePath, TestFilePathStrings);
        try
        {
            var sample = new BytesData
            {
                AdeId = 1,
                BytesText = Encoding.UTF8.GetBytes(new string('a', 100)),
                ZdexId = 2
            };
            persistService.Add(sample);
            Assert.Fail("An exception should be thrown when attempting to write a byte array that is too long.");
        }
        catch (Exception e)
        {
            Assert.AreEqual("One or more errors occurred. (The byte[] property 'BytesText' is too long.)", e.Message,
                "The exception message should match.");
        }
    }
}