using System.Text;
using Aiursoft.ArrayDb.Engine.Models;
using Aiursoft.ArrayDb.FilePersists;

namespace Aiursoft.ArrayDb.Engine.ObjectStorage;

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

    private long RequestWriteSpaceAndGetStartOffset(int length)
    {
        long writeOffset;
        lock (_expandSizeLock)
        {
            writeOffset = FileEndOffset;
            FileEndOffset += length;
            _fileAccess.WriteInFile(0, BitConverter.GetBytes(FileEndOffset));
        }
        return writeOffset;
    }
    
    public SavedString[] BulkWriteStringContentAndGetOffsets(byte[][] processedStrings)
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

        return result;
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