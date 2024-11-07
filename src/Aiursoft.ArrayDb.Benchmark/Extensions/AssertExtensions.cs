using Aiursoft.ArrayDb.Benchmark.Models;

namespace Aiursoft.ArrayDb.Benchmark.Extensions;

public static class AssertExtensions
{
    public static void EnsureCorrectness(this TestEntity[] entities, int shouldHaveItems, bool onlyValidateCount = false)
    {
        var actualCount = entities.Length;
        if (actualCount != shouldHaveItems)
        {
            throw new InvalidOperationException(
                $"The number of items is incorrect. Expected: {shouldHaveItems}, Actual: {actualCount}");
        }
        
        if (onlyValidateCount)
        {
            return;
        }
        
        for (var i = 0; i < actualCount; i+= new Random().Next(1, Program.OneKilo))
        {
            var (correct, error) = TestEntityFactory.IsCorrectlyFactoryCreated(entities[i], i);
            if (!correct)
            {
                throw new InvalidOperationException($"The item at index {i} is incorrect. " + error);
            }
        }
    }
}