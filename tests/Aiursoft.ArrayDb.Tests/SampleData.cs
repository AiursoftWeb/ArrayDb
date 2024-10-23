﻿namespace Aiursoft.ArrayDb.Tests;

public class SampleData
{
    public int MyNumber1 { get; init; }
    public string MyString1 { get; init; } = string.Empty;
    public int MyNumber2 { get; init; }
    public bool MyBoolean1 { get; init; }
    public string? MyString2 { get; init; }
    public DateTime MyDateTime { get; set; } = DateTime.UtcNow;
}