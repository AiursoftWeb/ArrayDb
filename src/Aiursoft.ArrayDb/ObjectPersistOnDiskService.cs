using System.Runtime.CompilerServices;

namespace Aiursoft.ArrayDb;

/// <summary>
/// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ObjectPersistOnDiskService<T> where T : new()
{
    public int Length;
    private readonly object _expandLengthLock = new();
    private const int LengthMarkerSize = sizeof(int); // We reserve the first 4 bytes for Length
    public readonly FileAccessService StructureFileAccess;
    public readonly StringRepository StringRepository;

    /// <summary>
    /// The ObjectPersistOnDiskService class provides methods to serialize and deserialize objects to and from disk. Making the disk can be accessed as an array of objects.
    /// </summary>
    /// <param name="structureFilePath">The path to the file that stores the structure of the objects.</param>
    /// <param name="stringFilePath">The path to the file that stores the string data.</param>
    /// <param name="initialSizeIfNotExists">The initial size of the file if it does not exist.</param>
    /// <typeparam name="T"></typeparam>
    public ObjectPersistOnDiskService(string structureFilePath, string stringFilePath, long initialSizeIfNotExists)
    {
        StructureFileAccess = new(structureFilePath, initialSizeIfNotExists);
        StringRepository = new(stringFilePath, initialSizeIfNotExists);
        Length = GetLength();
    }
    
    private int GetLength()
    {
        var buffer = StructureFileAccess.ReadInFile(0, LengthMarkerSize);
        return BitConverter.ToInt32(buffer, 0);
    }
    
    private void SaveLength(int length)
    {
        var buffer = BitConverter.GetBytes(length);
        StructureFileAccess.WriteInFile(0, buffer);
    }

    public int GetItemSize()
    {
        var size = 0;
        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(bool))
            {
                size += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                size += Unsafe.SizeOf<long>() * 2; // Offset and Length (2 long values)
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                size += Unsafe.SizeOf<long>(); // Size of DateTime (stored as Ticks, which is a long)
            }
            else
            {
                throw new Exception($"Unsupported property type: {prop.PropertyType}");
            }
        }
        return size;
    }

    private void SerializeBytes(T obj, byte[] buffer, int offset)
    {
        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(bool))
            {
                // Bool should be stored as 1 or 0
                var value = (bool)prop.GetValue(obj)! ? 1 : 0;
                Unsafe.WriteUnaligned(ref buffer[offset], value);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(int))
            {
                // Int should be stored as int
                var value = prop.GetValue(obj);
                Unsafe.WriteUnaligned(ref buffer[offset], (int)value!);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                // String should be stored as Offset (long) and Length (int)
                var stringValue = (string)prop.GetValue(obj)!;
                var (stringOffset, stringLength) = StringRepository.WriteStringContentAndGetOffset(stringValue);
                Unsafe.WriteUnaligned(ref buffer[offset], stringOffset);
                offset += Unsafe.SizeOf<long>();
                Unsafe.WriteUnaligned(ref buffer[offset], stringLength);
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                // DateTime should be stored as Ticks (long)
                var dateTimeValue = (DateTime)prop.GetValue(obj)!;
                var ticks = dateTimeValue.Ticks;
                Unsafe.WriteUnaligned(ref buffer[offset], ticks);
                offset += Unsafe.SizeOf<long>();
            }
            else
            {
                throw new Exception($"Unsupported property type: {prop.PropertyType}");
            }
        }
    }

    private T Deserialize(byte[] buffer, int offset = 0)
    {
        var obj = new T();

        foreach (var prop in typeof(T).GetProperties())
        {
            if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(bool))
            {
                var value = Unsafe.ReadUnaligned<int>(ref buffer[offset]);
                if (prop.PropertyType == typeof(bool))
                {
                    prop.SetValue(obj, value != 0);
                }
                else
                {
                    prop.SetValue(obj, value);
                }
                offset += Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(string))
            {
                // Read the Offset and Length
                var stringOffset = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                var stringLength = Unsafe.ReadUnaligned<int>(ref buffer[offset + Unsafe.SizeOf<long>()]);

                // Read the string from the string file
                var stringValue = StringRepository.LoadStringContent(stringOffset, stringLength);
                prop.SetValue(obj, stringValue);

                offset += Unsafe.SizeOf<long>() + Unsafe.SizeOf<int>();
            }
            else if (prop.PropertyType == typeof(DateTime))
            {
                var ticks = Unsafe.ReadUnaligned<long>(ref buffer[offset]);
                var dateTimeValue = new DateTime(ticks);
                prop.SetValue(obj, dateTimeValue);
                offset += Unsafe.SizeOf<long>();
            }
        }
        return obj;
    }

    private void WriteIndex(long index, T obj)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject];
        SerializeBytes(obj, buffer, 0);
        StructureFileAccess.WriteInFile(sizeOfObject * index + LengthMarkerSize, buffer);
    }
    
    private void WriteBulk(long index, T[] objs)
    {
        var sizeOfObject = GetItemSize();
        var buffer = new byte[sizeOfObject * objs.Length];
        Parallel.For(0, objs.Length, i =>
        {
            SerializeBytes(objs[i], buffer, sizeOfObject * i);
        });
        StructureFileAccess.WriteInFile(sizeOfObject * index + LengthMarkerSize, buffer);
    }
    
    public void Add(T obj)
    {
        int indexToWrite;
        lock (_expandLengthLock)
        {
            indexToWrite = Length;
            Length++;
        }   
        WriteIndex(indexToWrite, obj);
        
        // TODO: This should be done asynchronously to avoid blocking the main thread.
        SaveLength(Length);
    }
    
    public void AddBulk(T[] objs)
    {
        int indexToWrite;
        lock (_expandLengthLock)
        {
            indexToWrite = Length;
            Length += objs.Length;
        }
        WriteBulk(indexToWrite, objs);
        
        // TODO: This should be done asynchronously to avoid blocking the main thread.
        SaveLength(Length);
    }
    
    public T Read(long index)
    {
        var sizeOfObject = GetItemSize();
        var data = StructureFileAccess.ReadInFile(sizeOfObject * index + LengthMarkerSize, sizeOfObject);
        return Deserialize(data);
    }

    public T[] ReadBulk(long indexFrom, int count)
    {
        var sizeOfObject = GetItemSize();
        var data = StructureFileAccess.ReadInFile(sizeOfObject * indexFrom + LengthMarkerSize, sizeOfObject * count);
        var result = new T[count];
        Parallel.For(0, count, i =>
        {
            result[i] = Deserialize(data, sizeOfObject * i);
        });
        return result;
    }
}