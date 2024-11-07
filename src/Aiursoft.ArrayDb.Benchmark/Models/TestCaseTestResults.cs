using Aiursoft.ArrayDb.Benchmark.Abstractions;

namespace Aiursoft.ArrayDb.Benchmark.Models;

public class TestCaseTestResults
{
    public required ITestCase TestCase { get; init; }
    public required IEnumerable<TestResult> TestResults { get; init; }
}