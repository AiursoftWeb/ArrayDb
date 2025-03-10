using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Attributes;

namespace Aiursoft.ArrayDb.Tests.Base.Models;

public class BytesData
{
    public int AdeId { get; set; }
    
    [FixedLengthString(BytesLength = 50)]
    public byte[] BytesText { get; set; } = [];
    
    public int ZdexId { get; set; }
}