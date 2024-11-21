using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.ObjectBucket.Attributes;

namespace Aiursoft.ArrayDb.Tests.Base.Models;

public class BytesData : BucketEntity
{
    public int AdeId { get; set; }
    
    [FixedLengthString(BytesLength = 50)]
    public byte[] BytesText { get; set; } = [];
    
    public int ZdexId { get; set; }
}