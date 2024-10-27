using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public abstract class ArrayDbTestBase
{
    public static readonly object LockObject = new();

    [TestInitialize]
    public void Init()
    {
        File.Delete("sampleData.bin");
        File.Delete("sampleDataStrings.bin");
        Monitor.Enter(LockObject);

    }

    [TestCleanup]
    public void TestCleanup()
    {
        Monitor.Exit(LockObject);
    }
}