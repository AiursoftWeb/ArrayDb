namespace Aiursoft.ArrayDb.ObjectBucket;

public static class ObjectBucketExtensions
{
    public static IEnumerable<T> AsEnumerable<T>(this IObjectBucket<T> bucket,
        int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize) where T : BucketEntity, new()
    {
        // Copy the value to a local variable to avoid race condition. The ArchivedItemsCount may be changed by other threads.
        var archivedItemsCount = bucket.ArchivedItemsCount;
        for (var i = 0; i < archivedItemsCount; i += bufferedReadPageSize)
        {
            var readCount = Math.Min(bufferedReadPageSize, archivedItemsCount - i);
            var result = bucket.ReadBulk(i, readCount);
            foreach (var item in result)
            {
                yield return item;
            }
        }
    }
}