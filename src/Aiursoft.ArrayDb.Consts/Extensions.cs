namespace Aiursoft.ArrayDb.Consts;

public static class StringExtensions
{
    public static string AppendTabsEachLineHead(this string str)
    {
        return string.Join("\n", str.Split('\n').Select(line => $"\t{line}"));
    }
}