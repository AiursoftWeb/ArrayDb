using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests.Base;

[TestClass]
[DoNotParallelize]
public abstract class ArrayDbTestBase
{
    public static readonly object LockObject = new();

    [TestInitialize]
    public void Init()
    {
        Monitor.Enter(LockObject);
        if (File.Exists("sampleData.bin"))
            File.Delete("sampleData.bin");
        if (File.Exists("sampleDataStrings.bin"))
            File.Delete("sampleDataStrings.bin");
    }

    [TestCleanup]
    public void TestCleanup()
    {
        if (File.Exists("sampleData.bin"))
            File.Delete("sampleData.bin");
        if (File.Exists("sampleDataStrings.bin"))
            File.Delete("sampleDataStrings.bin");
        Monitor.Exit(LockObject);
    }
}