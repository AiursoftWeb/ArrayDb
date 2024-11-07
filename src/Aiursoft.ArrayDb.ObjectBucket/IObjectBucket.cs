namespace Aiursoft.ArrayDb.ObjectBucket;

public interface IObjectBucket<T> where T : BucketEntity, new()
{
    /// <summary>
    /// The count of the items in the bucket that can be read.
    ///
    /// In some cases, not all items might be persisted after calling `Add` method. And those items are not counted.
    /// </summary>
    int Count { get; }    
    void Add(params T[] objs);
    
    T Read(int index);
    
    T[] ReadBulk(int indexFrom, int take);
    
    Task DeleteAsync();
    
    string OutputStatistics();
}