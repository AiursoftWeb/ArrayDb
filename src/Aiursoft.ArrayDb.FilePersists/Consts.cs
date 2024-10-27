namespace Aiursoft.ArrayDb.FilePersists;

public static class Consts
{
    public const long DefaultPhysicalFileSize = 0x1000000; // 16 MB
    
    #region Read Cache
    public const int ReadCachePageSize = 0x1000000; // 16 MB

    public const int MaxReadCachedPagesCount = 0x40; // 64 pages cached in memory at most (1 GB)
    
    public const int ReadCacheHotCacheItems = 8; // most recent 8 pages are considered hot and will not be moved even they are used
    #endregion
    
    #region Write Buffer
    public const int MaxWriteBufferCachedItemsCount = 0x400000; // 4 million items
    
    public const int WriteBufferInitialCooldownMilliseconds = 1000; // 1 second
    
    public const int WriteBufferMaxCooldownMilliseconds = 1000 * 4; // 4 seconds
    #endregion

}