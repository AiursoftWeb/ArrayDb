using Aiursoft.ArrayDb.ObjectBucket;

namespace Aiursoft.ArrayDb.Benchmark.Models;

public class TestTarget
{
    public required string TestTargetName { get; init; }
    public required IObjectBucket<TestEntity> TestEntities { get; init; }
}