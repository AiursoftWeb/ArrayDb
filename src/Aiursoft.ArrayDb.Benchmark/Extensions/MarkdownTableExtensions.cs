using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.Extensions;

public static class MarkdownTableExtensions
{
    public static string ToMarkdownTable(List<TestCaseTestResults> results)
    {
        if (results.Count == 0)
        {
            return "No data available.";
        }

        var testItems = results
            .SelectMany(r => r.TestResults)
            .Select(tr => tr.TestedItem)
            .Distinct()
            .OrderBy(item => item)
            .ToList();

        // Header row: Test case + test items
        var header = "| Test Case | " + string.Join(" | ", testItems) + " |";
        var separator = "|---" + string.Concat(Enumerable.Repeat("|---", testItems.Count)) + "|";

        // Data rows
        var rows = results.Select(result =>
        {
            var row = new List<string> { result.TestCase.TestCaseName };
            foreach (var item in testItems)
            {
                // Find the matching test result, if any
                var matchingResult = result.TestResults.FirstOrDefault(tr => tr.TestedItem == item);
                if (matchingResult != null)
                {
                    var parallelTime = matchingResult.ParallelRunTime.HasValue
                        ? $"{matchingResult.ParallelRunTime.Value.TotalMilliseconds} ms (P)"
                        : string.Empty;
                    var serialTime = $"{matchingResult.SerialRunTime.TotalMilliseconds} ms (S)";
                    row.Add($"{serialTime}, {parallelTime}");
                }
                else
                {
                    row.Add(string.Empty);
                }
            }

            return "| " + string.Join(" | ", row) + " |";
        });

        // Combine header, separator, and data rows into a Markdown table
        return string.Join(Environment.NewLine, new[] { header, separator }.Concat(rows));
    }
}