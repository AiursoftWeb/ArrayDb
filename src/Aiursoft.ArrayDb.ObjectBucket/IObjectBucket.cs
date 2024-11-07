namespace Aiursoft.ArrayDb.ObjectBucket;

public interface IObjectBucket<T> where T : BucketEntity, new()
{
    int SpaceProvisionedItemsCount { get; }
    int ArchivedItemsCount { get; }
    void Add(params T[] objs);
    T Read(int index);
    T[] ReadBulk(int indexFrom, int count);
    Task DeleteAsync();
}