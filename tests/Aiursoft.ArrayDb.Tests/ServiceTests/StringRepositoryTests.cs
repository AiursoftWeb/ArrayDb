using System.Text;
using Aiursoft.ArrayDb.Tests.Base;

namespace Aiursoft.ArrayDb.Tests.ServiceTests;

[TestClass]
[DoNotParallelize]
public class StringRepositoryTests : ArrayDbTestBase
{
    [TestMethod]
    public void MultipleThreadsBulkWriteStringContentShouldBeThreadSafe()
    {
        // Path to a temporary file for testing
        var tempFilePath = TestFilePathStrings;

        // Initialize the StringRepository with suitable file and cache settings
        var stringRepository = new StringRepository.ObjectStorage.StringRepository(
            stringFilePath: tempFilePath,
            initialUnderlyingFileSizeIfNotExists: 0x10000,
            cachePageSize: 1024,
            maxCachedPagesCount: 100,
            hotCacheItems: 10);

        var threads = new List<Thread>();
        var expectedStrings = new List<(long Offset, int Length, string Content)>();
        var lockObject = new object();

        // Create and start threads
        for (var i = 0; i < 50; i++) // 50 threads for the test
        {
            var threadIndex = i;
            var thread = new Thread(() =>
            {
                var stringBytesList = new List<byte[]>();
                for (var j = 0; j < 20; j++) // Each thread writes 20 strings
                {
                    var text = $"Thread-{threadIndex}-String-{j}";
                    var bytes = Encoding.UTF8.GetBytes(text);
                    stringBytesList.Add(bytes);
                }

                // Perform bulk write in StringRepository
                var offsets = stringRepository.BulkWriteStringContentAndGetOffsets(stringBytesList.ToArray());

                // Add offsets and content to expected result for later verification
                lock (lockObject)
                {
                    for (var k = 0; k < offsets.Length; k++)
                    {
                        expectedStrings.Add((offsets[k].Offset, offsets[k].Length,
                            Encoding.UTF8.GetString(stringBytesList[k])));
                    }
                }
            });
            threads.Add(thread);
        }

        // Start all threads
        threads.ForEach(t => t.Start());

        // Wait for all threads to complete
        threads.ForEach(t => t.Join());

        // Verify all saved strings match their expected values
        foreach (var (offset, length, content) in expectedStrings)
        {
            var loadedContent = stringRepository.LoadStringContent(offset, length);
            Assert.AreEqual(content, loadedContent);
        }

        // Cleanup temporary file after test
        File.Delete(tempFilePath);
    }

    [TestMethod]
    public void BulkWriteAndReadSingleStringShouldBeCorrect()
    {
        var tempFilePath = TestFilePathStrings;

        var stringRepository = new StringRepository.ObjectStorage.StringRepository(
            stringFilePath: tempFilePath,
            initialUnderlyingFileSizeIfNotExists: 0x10000,
            cachePageSize: 1024,
            maxCachedPagesCount: 100,
            hotCacheItems: 10);

        var input = new[] { "Hello, ArrayDb!"u8.ToArray() };
        var offsets = stringRepository.BulkWriteStringContentAndGetOffsets(input);

        Assert.AreEqual(1, offsets.Length);
        Assert.AreEqual(input[0].Length, offsets[0].Length);

        var loaded = stringRepository.LoadStringContent(offsets[0].Offset, offsets[0].Length);
        Assert.AreEqual("Hello, ArrayDb!", loaded);

        File.Delete(tempFilePath);
    }

    [TestMethod]
    public void BulkWriteMultipleStringsShouldWriteAndReadCorrectly()
    {
        var tempFilePath = TestFilePathStrings;

        var stringRepository = new StringRepository.ObjectStorage.StringRepository(
            stringFilePath: tempFilePath,
            initialUnderlyingFileSizeIfNotExists: 0x10000,
            cachePageSize: 1024,
            maxCachedPagesCount: 100,
            hotCacheItems: 10);

        var inputs = new[]
        {
            "Hello"u8.ToArray(),
            "World"u8.ToArray(),
            "This is a longer string for testing purposes"u8.ToArray(),
            "Short"u8.ToArray(),
            ""u8.ToArray()
        };

        var offsets = stringRepository.BulkWriteStringContentAndGetOffsets(inputs);

        Assert.AreEqual(inputs.Length, offsets.Length);

        for (var i = 0; i < inputs.Length; i++)
        {
            Assert.AreEqual(inputs[i].Length, offsets[i].Length);
            var loaded = stringRepository.LoadStringContent(offsets[i].Offset, offsets[i].Length);
            Assert.AreEqual(Encoding.UTF8.GetString(inputs[i]), loaded, $"Mismatch at index {i}");
        }

        File.Delete(tempFilePath);
    }

    [TestMethod]
    public void BulkWriteEmptyArrayShouldReturnEmptyResult()
    {
        var tempFilePath = TestFilePathStrings;

        var stringRepository = new StringRepository.ObjectStorage.StringRepository(
            stringFilePath: tempFilePath,
            initialUnderlyingFileSizeIfNotExists: 0x10000,
            cachePageSize: 1024,
            maxCachedPagesCount: 100,
            hotCacheItems: 10);

        var inputs = Array.Empty<byte[]>();
        var offsets = stringRepository.BulkWriteStringContentAndGetOffsets(inputs);

        Assert.AreEqual(0, offsets.Length);

        File.Delete(tempFilePath);
    }

    [TestMethod]
    public void BulkWriteLargeBatchOfStringsShouldWork()
    {
        var tempFilePath = TestFilePathStrings;

        var stringRepository = new StringRepository.ObjectStorage.StringRepository(
            stringFilePath: tempFilePath,
            initialUnderlyingFileSizeIfNotExists: 0x10000,
            cachePageSize: 1024,
            maxCachedPagesCount: 100,
            hotCacheItems: 10);

        const int stringCount = 1000;
        var inputs = new byte[stringCount][];
        for (var i = 0; i < stringCount; i++)
        {
            var text = $"String-{i:D5}-padding-to-make-it-reasonable-size";
            inputs[i] = Encoding.UTF8.GetBytes(text);
        }

        var offsets = stringRepository.BulkWriteStringContentAndGetOffsets(inputs);

        Assert.AreEqual(stringCount, offsets.Length);

        for (var i = 0; i < stringCount; i++)
        {
            Assert.AreEqual(inputs[i].Length, offsets[i].Length, $"Length mismatch at index {i}");
            var loaded = stringRepository.LoadStringContent(offsets[i].Offset, offsets[i].Length);
            Assert.AreEqual(Encoding.UTF8.GetString(inputs[i]), loaded, $"Content mismatch at index {i}");
        }

        File.Delete(tempFilePath);
    }
}