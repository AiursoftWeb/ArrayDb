namespace Aiursoft.ArrayDb.Benchmark.Models;

public class TestResult
{
    public required string TestedItem { get; set; }
    public required TimeSpan? ParallelRunTime { get; set; }

    public required TimeSpan SerialRunTime { get; set; }
}