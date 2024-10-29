using System.Diagnostics.CodeAnalysis;
using System.Text;
using Aiursoft.ArrayDb.Consts;
using Aiursoft.ArrayDb.FilePersists;
using Aiursoft.ArrayDb.FilePersists.Services;
using Aiursoft.ArrayDb.ReadLruCache;
using Aiursoft.ArrayDb.StringRepository.Models;

namespace Aiursoft.ArrayDb.StringRepository.ObjectStorage;

/// <summary>
/// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
/// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
/// </summary>
public class StringRepository
{
    // Save the offset.
    public long FileEndOffset;
    private const int EndOffsetSize = sizeof(long);
    private readonly object _expandSizeLock = new();
    
    // Underlying storage
    private readonly CachedFileAccessService _fileAccess;
    
    // Statistics
    public int RequestWriteSpaceCount;
    public int LoadStringContentCount;
    public int BulkWriteStringsCount;
    
    [ExcludeFromCodeCoverage]
    public void ResetAllStatistics()
    {
        RequestWriteSpaceCount = 0;
        LoadStringContentCount = 0;
        BulkWriteStringsCount = 0;
    }
    
    public string OutputStatistics()
    {
        return $@"
String repository statistics:

* Logical file end offset (in MB): {FileEndOffset / 1024 / 1024}
* Request write space events count: {RequestWriteSpaceCount}
* Load string content events count: {LoadStringContentCount}
* Bulk write strings events count: {BulkWriteStringsCount}

Underlying cached file access service statistics:
{_fileAccess.OutputCacheReport().AppendTabsEachLineHead()}
";
    }

    /// <summary>
    /// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
    /// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
    /// </summary>
    public StringRepository(
        string stringFilePath, 
        long initialUnderlyingFileSizeIfNotExists, 
        int cachePageSize, 
        int maxCachedPagesCount,
        int hotCacheItems)
    {
        _fileAccess = new(
            path: stringFilePath,
            initialUnderlyingFileSizeIfNotExists: initialUnderlyingFileSizeIfNotExists,
            cachePageSize: cachePageSize,
            maxCachedPagesCount: maxCachedPagesCount,
            hotCacheItems: hotCacheItems);
        FileEndOffset = GetStringFileEndOffset();
    }

    private long GetStringFileEndOffset()
    {
        var buffer = _fileAccess.ReadInFile(0, EndOffsetSize);
        var offSet = BitConverter.ToInt64(buffer, 0);
        // When initially the file is empty, we need to reserve the first 8 bytes for EndOffset
        return offSet <= EndOffsetSize ? EndOffsetSize : offSet;
    }

    private long RequestWriteSpaceAndGetStartOffset(int length)
    {
        long writeOffset;
        lock (_expandSizeLock)
        {
            writeOffset = FileEndOffset;
            FileEndOffset += length;
            _fileAccess.WriteInFile(0, BitConverter.GetBytes(FileEndOffset));
        }
        Interlocked.Increment(ref RequestWriteSpaceCount);
        return writeOffset;
    }

    /// <summary>
    /// BulkWriteStringContentAndGetOffsets method is used for writing multiple strings' content into a file and retrieving their offsets within the file.
    /// It writes the processed strings in parallel for improved performance and thread safety.
    ///
    /// This method is thread-safe. You can call it from multiple threads simultaneously.
    /// </summary>
    /// <param name="processedStrings">An array of byte arrays representing the processed strings to be written.</param>
    /// <returns>An array of SavedString objects containing the offsets and lengths of each processed string in the file.</returns>
    public SavedString[] BulkWriteStringContentAndGetOffsets(byte[][] processedStrings) // Multi-thread safe
    {
        var allBytes = processedStrings.SelectMany(x => x).ToArray();
        var writeOffset = RequestWriteSpaceAndGetStartOffset(allBytes.Length);
        _fileAccess.WriteInFile(writeOffset, allBytes);
        var offset = writeOffset;
        var result = new SavedString[processedStrings.Length];
        var index = 0;
        foreach (var processedString in processedStrings)
        {
            result[index] = new SavedString { Offset = offset, Length = processedString.Length };
            offset += processedString.Length;
            index++;
        }

        Interlocked.Increment(ref BulkWriteStringsCount);
        return result;
    }

    /// <summary>
    /// LoadStringContent method is used for loading a string's content from a file based on its offset and length.
    /// It reads the string content from the file and returns it as a string.
    ///
    /// This method is thread-safe. You can call it from multiple threads simultaneously.
    /// </summary>
    /// <param name="offset">The offset of the string in the file.</param>
    /// <param name="length">The length of the string in bytes.</param>
    /// <returns>The string content of the specified offset and length.</returns>
    public string? LoadStringContent(long offset, int length)
    {
        Interlocked.Increment(ref LoadStringContentCount);
        switch (offset)
        {
            case -1:
                return string.Empty;
            case -2:
                return null;
            default:
            {
                var stringBytes = _fileAccess.ReadInFile(offset, length);
                return Encoding.UTF8.GetString(stringBytes);
            }
        }
    }
}