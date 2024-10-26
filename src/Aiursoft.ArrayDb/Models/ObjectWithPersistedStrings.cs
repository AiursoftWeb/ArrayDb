namespace Aiursoft.ArrayDb.Models;

public struct ObjectWithPersistedStrings<T>
{
    public required T Object;
    public required IEnumerable<SavedString> Strings;
}