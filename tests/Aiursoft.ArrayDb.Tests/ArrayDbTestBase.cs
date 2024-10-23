using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Aiursoft.ArrayDb.Tests;

[TestClass]
public abstract class ArrayDbTestBase
{
    [TestInitialize]
    public void Init()
    {
        File.Delete("sampleData.bin");
        File.Delete("sampleDataStrings.bin");
    }
}