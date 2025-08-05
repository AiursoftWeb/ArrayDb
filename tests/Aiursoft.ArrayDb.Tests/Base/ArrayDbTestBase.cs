namespace Aiursoft.ArrayDb.Tests.Base;

[TestClass]
[DoNotParallelize]
public abstract class ArrayDbTestBase
{
    public static readonly object LockObject = new();
    protected const string TestFilePath = "sampleData.bin";
    protected const string TestFilePathStrings = "sampleDataStrings.bin";

    [TestInitialize]
    public void Init()
    {
        if (File.Exists(TestFilePath))
            File.Delete(TestFilePath);
        if (File.Exists(TestFilePathStrings))
            File.Delete(TestFilePathStrings);
    }

    [TestCleanup]
    public void TestCleanup()
    {
        if (File.Exists(TestFilePath))
            File.Delete(TestFilePath);
        if (File.Exists(TestFilePathStrings))
            File.Delete(TestFilePathStrings);
    }
}
