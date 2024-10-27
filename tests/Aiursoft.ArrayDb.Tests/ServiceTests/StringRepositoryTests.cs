using System.Text;
using Aiursoft.ArrayDb.Tests.Base;
using Microsoft.VisualStudio.TestTools.UnitTesting;

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
        for (int i = 0; i < 50; i++) // 50 threads for the test
        {
            var threadIndex = i;
            var thread = new Thread(() =>
            {
                var stringBytesList = new List<byte[]>();
                for (int j = 0; j < 20; j++) // Each thread writes 20 strings
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
                    for (int k = 0; k < offsets.Length; k++)
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
}