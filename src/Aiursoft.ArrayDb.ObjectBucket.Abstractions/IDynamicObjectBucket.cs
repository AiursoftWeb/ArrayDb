namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions;

public interface IDynamicObjectBucket
{
    int Count { get; }
    void Add(params BucketItem[] objs);
    BucketItem Read(int index);
    BucketItem[] ReadBulk(int indexFrom, int take);
    Task DeleteAsync();
    string OutputStatistics();
    Task SyncAsync();
}