using System.Text;

namespace Aiursoft.ArrayDb;

public struct StringInByteArray
{
    public required long Offset;
    public required int Length;
}

/// <summary>
/// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
/// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
/// </summary>
public class StringRepository
{
    private readonly CachedFileAccessService _fileAccess;
    public long FileEndOffset;
    private readonly object _expandSizeLock = new();
    private const int EndOffsetSize = sizeof(long); // We reserve the first 8 bytes for EndOffset

    /// <summary>
    /// StringRepository is a class designed to handle the storage and retrieval of string data within a specified file.
    /// It manages the strings' offsets in the file and provides methods to save new strings and retrieve existing ones.
    /// </summary>
    public StringRepository(string stringFilePath, long initialSizeIfNotExists)
    {
        _fileAccess = new(stringFilePath, initialSizeIfNotExists);
        FileEndOffset = GetStringFileEndOffset();
    }

    private long GetStringFileEndOffset()
    {
        var buffer = _fileAccess.ReadInFile(0, EndOffsetSize);
        var offSet = BitConverter.ToInt64(buffer, 0);
        // When initially the file is empty, we need to reserve the first 8 bytes for EndOffset
        return offSet <= EndOffsetSize ? EndOffsetSize : offSet;
    }

    /// <summary>
    /// This method writes a string to the file and returns the offset where the string is stored.
    ///
    /// This method is multi-thread safe.
    /// </summary>
    /// <param name="str"></param>
    /// <returns></returns>
    private StringInByteArray WriteStringContentAndGetOffset(string? str)
    {
        switch (str)
        {
            case "":
                // -1 offset indicates empty string
                return new StringInByteArray { Offset = -1, Length = 0 };
            case null:
                return new StringInByteArray { Offset = -2, Length = 0 }; // -2 offset indicates null string
        }

        var stringBytes = Encoding.UTF8.GetBytes(str);
        long writeOffset;
        lock (_expandSizeLock) // When calling this method multi-threads, different threads are writing to different offsets
        {
            writeOffset = FileEndOffset;
            FileEndOffset += stringBytes.Length;
        }
        // TODO IMMEDIATELY: Write all strings in a single write operation
        // Then calculate the offset of each string
        _fileAccess.WriteInFile(writeOffset, stringBytes);
        return new StringInByteArray { Offset = writeOffset, Length = stringBytes.Length };
        // Warning, DO NOT CALL this method without updating the end offset in the string file.
    }

    // TODO: Use multiple underlying files (partitions) to store strings
    // So we can use multiple threads to write strings concurrently
    public StringInByteArray[] BulkWriteStringContentAndGetOffset(IEnumerable<string> stringsQuery, int stringsCount)
    {
        var stringInByteArrays = new StringInByteArray[stringsCount];
        var index = 0;
        foreach (var str in stringsQuery)
        {
            stringInByteArrays[index++] = WriteStringContentAndGetOffset(str);
        }
        
        // Update the end offset in the string file
        _fileAccess.WriteInFile(0, BitConverter.GetBytes(FileEndOffset));
        
        return stringInByteArrays;
    }

    public IEnumerable<StringInByteArray> BulkWriteStringContentAndGetOffsetV2(ProcessingString[] processedStrings)
    {
        // This version, we fetch all strings and save it in a byte array
        // Then we write the byte array to the file
        // Then we calculate the offset of each string
        var allBytes = processedStrings.SelectMany(p => p.Bytes).ToArray();
        var writeOffset = FileEndOffset;
        _fileAccess.WriteInFile(writeOffset, allBytes);
        var offset = writeOffset;
        foreach (var processedString in processedStrings)
        {
            var stringInByteArray = new StringInByteArray { Offset = offset, Length = processedString.Length };
            offset += processedString.Length;
            yield return stringInByteArray;
        }
        FileEndOffset = offset;
        _fileAccess.WriteInFile(0, BitConverter.GetBytes(FileEndOffset));
    }

    public string? LoadStringContent(long offset, int length)
    {
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

public class ProcessingString
{
    public int Length;
    public required byte[] Bytes;
}