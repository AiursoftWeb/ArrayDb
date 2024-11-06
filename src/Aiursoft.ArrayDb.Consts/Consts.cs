namespace Aiursoft.ArrayDb.Consts;

public static class Consts
{
    /// <summary>
    /// The default physical file size for a partitioned object bucket. Default is 16 MB.
    ///
    /// If this was set too large, a lot of partitions may consume a lot of disk space. (For example, 1024 partitions will cause 16 GB disk space consumed)
    ///
    /// If this was set too small, the data will be fragmented and the disk will be heavily used.
    ///
    /// Suggested value = (Expected final file size) / (Number of partitions)
    ///
    /// For example, if you don't want to consume more than 32GB, and you may have 2048 partitions, then the value should be 32GB / 2048 = 16MB
    /// </summary>
    public const long DefaultPhysicalFileSize = 0x1000000;
    
    #region Read Cache
    
    /// <summary>
    /// The size of a page in the read cache. Default is 16 MB.
    ///
    /// If this was set too large, the memory will be consumed quickly.
    ///
    /// If this was set too small, the disk will be heavily used.
    ///
    /// Suggested value equals or smaller than the physical file size.
    ///
    /// For example, if there are 64 pages cached in memory at most, and each page is 16 MB, then the memory consumed will be 64 * 16 = 1024 MB
    /// </summary>
    public const int ReadCachePageSize = 0x1000000;

    /// <summary>
    /// The maximum number of pages cached in memory at most. Default is 64 pages.
    ///
    /// If this was set too large, the memory will be consumed quickly.
    ///
    /// If this was set too small, the cache will be dropped frequently.
    ///
    /// Suggested value: 8-256 pages.
    ///
    /// For example, if there are 64 pages cached in memory at most, and each page is 16 MB, then the memory consumed will be 64 * 16 = 1024 MB
    /// </summary>
    public const int MaxReadCachedPagesCount = 0x40;
    
    /// <summary>
    /// The number of hot cache items. Default is 8.
    ///
    /// When the cache was hit, and it will only be moved to the end of the cache list, if it was not a hot cache item.
    ///
    /// Setting this too large may cause determining the hot cache items slowly.
    ///
    /// Setting this too small may cause the cache to be swapped frequently.
    ///
    /// The value doesn't impact the memory usage.
    ///
    /// Suggestions: 4-16. Must be smaller than MaxReadCachedPagesCount.
    /// </summary>
    public const int ReadCacheHotCacheItems = 0x8; // most recent 8 pages are considered hot and will not be moved even they are used
    #endregion
    
    
    #region Write Buffer

    /// <summary>
    /// The initial cooldown time in milliseconds before a write operation is triggered. Default is 1000 ms (1 second).
    /// 
    /// A shorter cooldown time reduces latency but may increase fragmentation and disk operations.
    /// A longer cooldown time can help batch data but may delay persistence.
    /// 
    /// Suggested value: 1000 ms, to allow a balance between latency and fragmentation.
    /// </summary>
    public const int WriteBufferCooldownMilliseconds = 1000;
    #endregion

    /// <summary>
    /// When reading as enumerable, we couldn't read items one by one. Because it's slow.
    ///
    /// We will read items in a batch. This is the size of the batch.
    ///
    /// For example, if the page size is 128, and there are totally 2000 items in the bucket. Then there will be 2 pages. The first phase will read 128 items, and the second phase will read 72 items.
    ///
    /// Default value is 128.
    ///
    /// Setting this too large may cause low performance when only need to read a few items.
    ///
    /// Setting this too small may cause repeated disk reading and result in low performance.
    /// </summary>
    public const int AsEnumerablePageSize = 0x80;
}