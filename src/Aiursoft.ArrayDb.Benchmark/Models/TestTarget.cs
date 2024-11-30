using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;

namespace Aiursoft.ArrayDb.Benchmark.Models;

public class TestTarget
{
    public required string TestTargetName { get; init; }
    public required Func<IObjectBucket<TestEntity>> TestEntities { get; init; }
}