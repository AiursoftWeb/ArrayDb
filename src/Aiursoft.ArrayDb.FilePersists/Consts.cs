namespace Aiursoft.ArrayDb.FilePersists;

public static class Consts
{
    public const long DefaultFileSize = 0x1000000; // 16 MB
    
    public const int CachePageSize = 0x1000000; // 16 MB

    public const int MaxCachedPagesCount = 0x40; // 64 pages cached in memory at most (1 GB)
    
    public const int HotCacheItems = 8; // most recent 8 pages are considered hot and will not be moved even they are used
}