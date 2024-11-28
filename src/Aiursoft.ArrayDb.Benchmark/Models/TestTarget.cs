using Aiursoft.ArrayDb.ObjectBucket;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions;

namespace Aiursoft.ArrayDb.Benchmark.Models;

public class TestTarget
{
    public required string TestTargetName { get; init; }
    public required Func<IObjectBucket<TestEntity>> TestEntities { get; init; }
}