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
            // Calculate the range of indices to fetch
            var startIndex = Math.Max(0, i - bufferedReadPageSize + 1);
            var readCount = i - startIndex + 1;

            // Read the bulk data from the bucket
            var result = bucket.ReadBulk(startIndex, readCount);

            // Manually iterate over the result in reverse order
            for (int j = result.Length - 1; j >= 0; j--)
            {
                yield return result[j];
            }
        }
    }
}