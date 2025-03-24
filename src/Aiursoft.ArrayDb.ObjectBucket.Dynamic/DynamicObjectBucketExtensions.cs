using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Models;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic.Simplify;

namespace Aiursoft.ArrayDb.ObjectBucket.Dynamic;

public static class DynamicObjectBucketExtensions
{
    public static IEnumerable<dynamic> AsSimplified(this IDynamicObjectBucket bucket)
    {
        return bucket.AsEnumerable().Select(dynamic (item) => new SimplifiedBucketItem(item));
    }

    public static IEnumerable<BucketItem> AsEnumerable(this IDynamicObjectBucket bucket,
        int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize)
    {
        // Copy the count locally to avoid race conditions if the bucket is modified concurrently.
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

    public static IEnumerable<BucketItem> AsReverseEnumerable(this IDynamicObjectBucket bucket,
        int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize)
    {
        // Copy the count locally to avoid race conditions.
        var archivedItemsCount = bucket.Count;
        for (var i = archivedItemsCount - 1; i >= 0; i -= bufferedReadPageSize)
        {
            // Calculate the starting index for this page.
            var startIndex = Math.Max(0, i - bufferedReadPageSize + 1);
            var readCount = i - startIndex + 1;
            var result = bucket.ReadBulk(startIndex, readCount);
            // Iterate over the page in reverse order.
            for (var j = result.Length - 1; j >= 0; j--)
            {
                yield return result[j];
            }
        }
    }
}
