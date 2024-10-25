using System.Text;

namespace Aiursoft.ArrayDb;

public class StringInByteArray
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

        _fileAccess.WriteInFile(FileEndOffset, stringBytes);
        FileEndOffset += stringBytes.Length;
        
        return new StringInByteArray
        { 
            Offset = FileEndOffset - stringBytes.Length, 
            Length = stringBytes.Length 
        };
        
        // Warning, DO NOT CALL this method without updating the end offset in the string file.
    }

    public IEnumerable<StringInByteArray> BulkWriteStringContentAndGetOffset(IEnumerable<string> strs)
    {
        foreach (var str in strs)
        {
            yield return WriteStringContentAndGetOffset(str);
        }
        
        // Update the end offset in the string file
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