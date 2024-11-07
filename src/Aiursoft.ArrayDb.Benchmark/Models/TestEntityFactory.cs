namespace Aiursoft.ArrayDb.Benchmark.Models;

public static class TestEntityFactory
{
    private static TestEntity Create(int i)
    {
        return new TestEntity
        {
            Id = i,
            ChineseString = $"Hello, World! 你好世界 {i}",
            Id10Times = i * 10,
            ThreadId = i % 10,
            IsEven = i % 2 == 0,
            NullableEnglishString = i % 7 == 0 ?
                string.Empty :
                $"This is another longer string. {i}"
        };
    }

    public static TestEntity[] CreateSome(int itemsCount)
    {
        var result = new TestEntity[itemsCount];
        for (var i = 0; i < itemsCount; i++)
        {
            result[i] = Create(i);
        }
        return result;
    }
    
    public static (bool success, string reason) IsCorrectlyFactoryCreated(TestEntity entity, int i)
    {
        var success = entity.Id == i &&
               entity.ChineseString == $"Hello, World! 你好世界 {i}" &&
               entity.Id10Times == i * 10 &&
                entity.ThreadId == i % 10 &&
               entity.IsEven == (i % 2 == 0) &&
               entity.NullableEnglishString == (i % 7 == 0 ?
                   string.Empty :
                   $"This is another longer string. {i}");

        if (!success)
        {
            var reason = $"Entity is not correctly factory created. Id: {entity.Id}, ChineseString: {entity.ChineseString}, Id10Times: {entity.Id10Times}, IsEven: {entity.IsEven}, NullableEnglishString: {entity.NullableEnglishString}";
            var shouldBe = Create(i);
            var finalReason = $"{reason} Should be: Id: {shouldBe.Id}, ChineseString: {shouldBe.ChineseString}, Id10Times: {shouldBe.Id10Times}, IsEven: {shouldBe.IsEven}, NullableEnglishString: {shouldBe.NullableEnglishString}";
            return (false, finalReason);
        }
        
        return (true, string.Empty);
    }
}