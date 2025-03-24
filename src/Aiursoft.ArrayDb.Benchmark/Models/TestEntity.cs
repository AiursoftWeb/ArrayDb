namespace Aiursoft.ArrayDb.Benchmark.Models;

public class TestEntity
{
    public int ThreadId { get; set; }
    public int Id { get; init; }
    public string ChineseString { get; init; } = string.Empty;
    public int Id10Times { get; init; }
    public bool IsEven { get; init; }
    public string? NullableEnglishString { get; init; }
}
