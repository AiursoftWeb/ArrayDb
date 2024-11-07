namespace Aiursoft.ArrayDb.Benchmark;

public static class TestEntityFactory
{
    public static TestEntity Create(int i)
    {
        return new TestEntity()
        {
            Id = i,
            ChineseString = $"Hello, World! 你好世界 {i}",
            Id10Times = i * 10,
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

    
    public static bool IsCorrectlyFactoryCreated(TestEntity entity, int i)
    {
        return entity.Id == i &&
               entity.ChineseString == $"Hello, World! 你好世界 {i}" &&
               entity.Id10Times == i * 10 &&
               entity.IsEven == (i % 2 == 0) &&
               entity.NullableEnglishString == (i % 7 == 0 ?
                   string.Empty :
                   $"This is another longer string. {i}");
    }
}