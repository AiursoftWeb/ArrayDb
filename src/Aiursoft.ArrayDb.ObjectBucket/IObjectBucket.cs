namespace Aiursoft.ArrayDb.ObjectBucket;

public interface IObjectBucket<T> where T : BucketEntity, new()
{
    /// <summary>
    /// SpaceProvisionedItemsCount is the total number of items that the bucket can store.
    ///
    /// When requesting to write new item, it will start to write at the offset of SpaceProvisionedItemsCount.
    /// 
    /// SpaceProvisionedItemsCount is always larger than or equal to ArchivedItemsCount.
    /// </summary>
    int SpaceProvisionedItemsCount { get; }
    
    /// <summary>
    /// ArchivedItemsCount is the total number of items that have been written to the bucket. Which are the items ready to be read.
    ///
    /// When finished writing new item, the ArchivedItemsCount will be increased by 1.
    ///
    /// ArchivedItemsCount is always less than or equal to SpaceProvisionedItemsCount.
    /// </summary>
    int ArchivedItemsCount { get; }
    
    void Add(params T[] objs);
    
    T Read(int index);
    
    T[] ReadBulk(int indexFrom, int count);
    
    Task DeleteAsync();
    
    string OutputStatistics();
}