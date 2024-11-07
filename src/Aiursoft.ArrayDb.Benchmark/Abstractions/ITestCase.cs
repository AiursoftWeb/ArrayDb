using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.Abstractions;

public interface ITestCase
{
    public string TestCaseName { get; }
    public Task<TestResult> RunAsync(TestTarget target);
}