namespace Aiursoft.ArrayDb.ObjectBucket;

public static class ObjectBucketExtensions
{
    public static IEnumerable<T> AsEnumerable<T>(this IObjectBucket<T> bucket,
        int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize) where T : BucketEntity, new()
    {
        // Copy the value to a local variable to avoid race condition. The ArchivedItemsCount may be changed by other threads.
        var archivedItemsCount = bucket.Count;
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

    public static IEnumerable<T> AsReverseEnumerable<T>(this IObjectBucket<T> bucket,
        int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize) where T : BucketEntity, new()
    {
        // Copy the value to a local variable to avoid race condition. The ArchivedItemsCount may be changed by other threads.
        var archivedItemsCount = bucket.Count;
        for (var i = archivedItemsCount - 1; i >= 0; i -= bufferedReadPageSize)
        {
            var readCount = Math.Min(bufferedReadPageSize, i + 1);
            var result = bucket.ReadBulk(i - readCount + 1, readCount);
            foreach (var item in result)
            {
                yield return item;
            }
        }
    }
}