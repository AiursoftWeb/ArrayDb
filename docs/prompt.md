下面的英文文档，是我之前开发的 ArrayDb 的文档。

ArrayDb 是一个数据库。但是，很遗憾的是，不同于 MySQL 它是个进程，ArrayDb 目前还是个库。它的用法如下：

=====================================

## How to use ArrayDb

Before starting, you need to install [.NET 10 SDK](https://dot.net) on your machine.

Unlike MySQL, working as a process, ArrayDb works as a library. You can use ArrayDb in your C# project by adding the `ArrayDb` NuGet package to your project.

```bash
mkdir LearnArrayDb
cd LearnArrayDb
dotnet new console
dotnet add package Aiursoft.ArrayDb.Partitions
```

That's it. Now you have ArrayDb in your project.

### Building the module

You can start using it by creating a new entity with type: `PartitionedBucketEntity<T>`, where T is the partition key type.

Supported property types are:

* `int`
* `bool`
* `string`
* `DateTime`
* `long`
* `float`
* `double`
* `TimeSpan`
* `Guid`
* Fixed length `byte[]`

```csharp
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Attributes;
using Aiursoft.ArrayDb.Partitions;

public class MyLogItem : PartitionedBucketEntity<string>
{
    [PartitionKey] 
    public string ApplicationName { get; set; } = string.Empty;

    [PartitionKey]
    public override string PartitionId
    {
        get => ApplicationName;
        set => ApplicationName = value;
    }
    
    public DateTime HappenTime { get; set; } = DateTime.UtcNow;

    public string LogMessage { get; set; } = string.Empty;

    public int HttpResponseCode { get; set; }

    public string RequestPath { get; set; } = string.Empty;
    
    public TimeSpan ResponseTime { get; set; }
    
    [FixedLengthString(BytesLength = 50)]
    public byte[] BytesText { get; set; } = [];
}
```

Then you can start using ArrayDb by creating a new `PartitionedBucket<T>` instance.

```csharp
var databaseName = "my-db";
var databaseFilePath = "/tmp/my-db";
Directory.CreateDirectory(databaseFilePath);
        
var db = new PartitionedObjectBucket<MyLogItem, string>(databaseName, databaseFilePath);
```

### Writing data

Now you can start using the `db` instance to write some data.

```csharp
// Write to the database.
db.Add(new MyLogItem
{
    ApplicationName = "NextCloud",
    LogMessage = "A user logged in.",
    HttpResponseCode = 200,
    RequestPath = "/account/login",
    ResponseTime = TimeSpan.FromMilliseconds(100)
});

db.Add(new MyLogItem
{
    ApplicationName = "NextCloud",
    LogMessage = "A user logged out.",
    HttpResponseCode = 200,
    RequestPath = "/account/logout",
    ResponseTime = TimeSpan.FromMilliseconds(50)
});

db.Add(new MyLogItem
{
    ApplicationName = "GitLab",
    LogMessage = "A user created a new project.",
    HttpResponseCode = 201,
    RequestPath = "/projects/new",
    ResponseTime = TimeSpan.FromMilliseconds(200)
});

db.Add(new MyLogItem
{
    ApplicationName = "Jellyfin",
    LogMessage = "Server crashed when playing a video.",
    HttpResponseCode = 500,
    RequestPath = "/play/video",
    ResponseTime = TimeSpan.FromMilliseconds(500)
});
```

And you can use bulk write to improve performance.

```csharp
var logs = new List<MyLogItem>();
for (var i = 0; i < 100; i++)
{
    logs.Add(new MyLogItem
    {
        ApplicationName = "HomeAssistant",
        LogMessage = $"A human was detected by the camera {i}.",
        HttpResponseCode = 200,
        RequestPath = $"camera/{i}/detect",
        ResponseTime = TimeSpan.FromMilliseconds(100)
    });
}
// Write 100 items at once.
db.Add(logs.ToArray());
```

Calling `SyncAsync()` is **optional**. It will block current thread and flush the data to the disk. However, if you don't call it, the data will also be archived very soon. Only call this to ensure the data is written to the disk.

```csharp
await db.SyncAsync();
```

### Reading data

You can read data from the database by using the `db` instance. For example, if you want to read from a specific partition and index, you can simply call `Read` with the partition key and index.

```csharp
// Read a specific item.
var specificLog = db.Read(partitionKey: "NextCloud", index: 1);
Console.WriteLine($"[{specificLog.HappenTime}] {specificLog.LogMessage}");
```

Calling `Read` has low performance when you need to read a large amount of data. You can use `ReadBulk` to read bulk data.

```csharp
// Bulk read logs from a specific partition.
var nextCloudLogs = db.ReadBulk(
    partitionKey: "NextCloud",
    indexFrom: 0,
    count: 2);

foreach (var log in nextCloudLogs)
{
    Console.WriteLine($"[{log.HappenTime}] {log.LogMessage}");
}
```

You may also want to know how many logs are there in a specific partition. You can use `Count` to get the count of logs in a specific partition.

```csharp
var nextCloudLogsCount = db.Count("NextCloud");
Console.WriteLine("NextCloud logs count: " + nextCloudLogsCount);
```

You can also read the data as an `IEnumerable` by using `AsEnumerable` with a partition key.

```csharp
var results = db.AsEnumerable(partitionKey: "NextCloud")
    .Where(t => t.HttpResponseCode == 200)
    .OrderBy(t => t.HappenTime)
    .Take(10)
    .ToArray();
```

However, using ArrayDb as an enumerable collection doesn't fully utilize its optimized performance characteristics. Thanks to its fixed-length structure, ArrayDb can quickly locate an element by index without additional overhead. If you need to enumerate through every element in the database, you might want to consider accessing data by index or in bulk where possible to leverage ArrayDb's constant-time (O(1)) access.

If you want to get all data from all partitions, you can use `ReadAll` to get all data.

```csharp
// (Not recommended for large data)
var allLogs = db.ReadAll();
Console.WriteLine("All logs count: " + allLogs.Length);
```

### Deleting data

ArrayDb only support deleting data by partition key. You can use `DeletePartition` to delete all data in a specific partition.

```csharp
// Delete a specific partition.
await db.DeletePartitionAsync("HomeAssistant");
var allLogsAfterDelete = db.ReadAll();
Console.WriteLine("All logs count after delete: " + allLogsAfterDelete.Length);
```

## Best practice

### Avoiding multiple processes accessing the same file!!!

Can I use ArrayDb in multiple processes or instance with the same underlining file?

Answer is: **Absolutely NO**. The underlining file is not thread-safe. You should not use the same file in multiple processes or instances.

So **avoid** doing this:

```csharp
// WRONG CODE, DO NOT COPY!!!
var dbInstanceA = new PartitionedObjectBucket<MyLogItem, string>("my-db", dbPath);
var dbInstanceB = new PartitionedObjectBucket<MyLogItem, string>("my-db", dbPath);

dbInstanceA.Add(new MyLogItem { ApplicationName = "NextCloud", LogMessage = "A user logged in." });
var count = dbInstanceB.Count("NextCloud"); // This will not work as expected!!!
```

If you have multiple services need to access the same data, you should use a server-client model. You can create a server with ArrayDb SDK to manage the data and let the clients access the data through the server.

### Default partition key

In some cases, you don't want to rename the `PartitionId` property to `ApplicationName` in the entity. You can directly add your own property. And use `PartitionId` to access the partition key.

```csharp
// This class inherits from PartitionedBucketEntity<string>, so PartitionId is the partition key.
public class MyLogItem : PartitionedBucketEntity<string>
{
    // Fill your own properties here.
    public string ApplicationName { get; set; } = string.Empty;

    public DateTime HappenTime { get; set; } = DateTime.UtcNow;

    public string LogMessage { get; set; } = string.Empty;

    public int HttpResponseCode { get; set; }

    public string RequestPath { get; set; } = string.Empty;
    
    public TimeSpan ResponseTime { get; set; }
    
    [FixedLengthString(BytesLength = 50)]
    public byte[] BytesText { get; set; } = [];
}

var log = new MyLogItem
{
    PartitionId = "NextCloud",
    LogMessage = "A user logged in.",
    HttpResponseCode = 200,
    RequestPath = "/account/login",
    ResponseTime = TimeSpan.FromMilliseconds(100)
};
```

### Rebooting

If your application reboots or crashed, you can simply create a new `PartitionedObjectBucket` instance with the same database name and file path to recover the data.

```csharp
var db = new PartitionedObjectBucket<Log, string>("my-db", dbPath);
for (var i = 0; i < 100; i++)
{
    var sample = new Log
    {
        Message = $"Hello, World! 你好世界 {i}",
        PartitionId = 0
    };
    partitionedService.Add(sample);
}
await partitionedService.SyncAsync(); // Make sure the data is written to the disk.

// Now the application crashes. After rebooting, you can still get the data.

var db = new PartitionedObjectBucket<Log, string>("my-db", dbPath);
foreach (var log in db.AsEnumerable(0))
{
    Console.WriteLine(log.Message);
}
```

However, it is still strongly recommended to keep the `PartitionedObjectBucket` as a singleton in your application. It has inner cache and will improve the performance.

Rebooting the instance will not lose any data before `SyncAsync` is called. But all cache will be lost. So it is better to keep the `PartitionedObjectBucket` instance alive and singleton.

### Using ArrayDb with Dependency Injection

Of course, you can use ArrayDb with Dependency Injection. You can create a singleton service to manage the `PartitionedObjectBucket` instance.

```csharp
services.AddSingleton<PartitionedObjectBucket<MyLogItem, string>>(provider =>
{
    var dbPath = Path.Combine(Directory.GetCurrentDirectory(), "my-db");
    return new PartitionedObjectBucket<MyLogItem, string>("my-db", dbPath);
});
```

Then you can inject the `PartitionedObjectBucket` from the DI container.

=====================================

虽然这个项目非常完美，并且已经实际上线落地，并且运行的非常稳定，但是上面的文档直接凸显了问题：

ArrayDb 是一个库，不是一个进程。它是一个库，所以你可以在你的 C# 项目中使用它。但是，它不支持多进程访问同一个文件。这意味着，如果你的应用程序需要多个进程访问同一个文件，你需要自己实现一个服务端，然后让客户端通过服务端访问数据。

所以，我最近计划开发一个自己的服务端。这个服务端可以像 MySQL 一样，支持多个客户端同时访问同一个数据库。我们最终需要构建的是一个真正的数据库，而不是一个库。这样才能将它投入到例如记录所有 HTTP 请求、记录所有日志、记录所有用户行为等等的场景中。

为了开发这个服务端，首先我需要定义一个自己的查询语言：ArrayQL。ArrayQL 是一个类 SQL 的查询语言，但是它只支持查询，不支持修改。ArrayQL 的语法如下：

```csharp
MyTable
.Skip(10)  
.Take(20)
```

上面的语句表示从 MyTable 表中跳过 10 条数据，然后取 20 条数据。

当然，为了查询第10到第29行的数据，你可以这么写：

```csharp
MyTable[10..30]
```

> 注意，Array[10..30] 表示从 10 开始，到 30 结束，但是不包括 30。这已经是 C# 原生支持的语法了。你不需要自己实现这个功能。

我会提供更多示例，例如：

查询 MyTable 的特定行：

```csharp
MyTable[10]
```

> 注意，上面的表达式会返回一个单独的行，而不是一个数组。因此，你需要特别小心：如果用户查询的是一个不存在的行，你需要返回一个空数组，而不是一个空行。如果用户查询的结果是一个单行，你也需要将其转换为一个只有一个元素的数组。

显然，上面这些方法，都是 C# 原生的方法。

查询我的所有 Customer 表中，资产大于 10 亿美元的美国客户或者年龄为 18 岁的非程序员客户的数量。

```csharp
Customers
.Where(c => c.Assets > 1000000000 && c.Country == "USA" || c.Age == 18 && c.Occupation != "Programmer")
.Count()
```

查询我的所有 Customer 表中，按照国家分组，统计每个国家的客户数量。

```csharp
Customers
.GroupBy(c => c.Country)
.Select(c => new { Country = c.Key, Count = c.Count() })
```

查询我的所有 HTTP 请求中，最近3天的，所有向 Nextcloud 的请求里，每天成功和失败的请求数量。

```csharp
HttpRequests
.Where(r => r.RequestTime > DateTime.Now.AddDays(-3))
.Where(r => r.Target == "Nextcloud")
.GroupBy(r => r.RequestTime.Date)
.Select(r => new { Date = r.Key, Success = r.Count(r => r.StatusCode == 200), Failed = r.Count(r => r.StatusCode != 200) })
```

看得出来，这个语言和 C# 的 LINQ 很像。但是，ArrayQL 是一个查询语言，不是一个编程语言。它只支持查询，不支持修改。它不支持变量，不支持循环，不支持递归，不支持异常处理。它虽然支持 Lambda 表达式，但是只支持一部分 C# 的 Lambda 表达式。

ArrayQl 支持的功能是 C# 的子集。

所以，为了完成上面的工作，我决定这么设计它的语法解析器。

首先，语法解析器一定是一个类，它的核心函数：Run，需要输入一个 string，作为 ArrayQL 的查询语句，输入一个 IDynamicObjectBucket，作为查询的数据源，输出一个 IEnumerable<P>，作为查询的结果。

其中，IDynamicObjectBucket 是 ArrayDb 核心对外暴露的接口，用于表示一个数据库表。它的定义如下：

```csharp
namespace Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;

public interface IDynamicObjectBucket
{
    int Count { get; }
    void Add(params BucketItem[] objs);
    BucketItem Read(int index);
    BucketItem[] ReadBulk(int indexFrom, int take);
    Task DeleteAsync();
    string OutputStatistics();
    Task SyncAsync();
}
```

看起来它无法被过滤、聚合，对吧？别担心，我已经为你写好了扩展方法，你可以直接使用它们。

```csharp
namespace Aiursoft.ArrayDb.ObjectBucket.Dynamic;

public static IEnumerable<BucketItem> AsEnumerable(this IDynamicObjectBucket bucket,
    int bufferedReadPageSize = Consts.Consts.AsEnumerablePageSize)
{
    // Copy the count locally to avoid race conditions if the bucket is modified concurrently.
    var archivedItemsCount = bucket.Count;
    for (var i = 0; i < archivedItemsCount; i += bufferedReadPageSize)
    {
        var readCount = Math.Min(bufferedReadPageSize, archivedItemsCount - i);
        var result = bucket.ReadBulk(i, readCount);
        foreach (var item in result)
        {
            yield return item;
        }
    }
}
```

我的扩展方法会帮你实现分页读取，你只需要调用 AsEnumerable，然后使用 LINQ 进行过滤、聚合就可以了。而且，这个方法是延迟加载的，所以不会一次性加载所有数据到内存中。

另外，我知道你一定对其中的一些引用到的类型不熟悉。所以，我会给你一些帮助。

其中，BucketItem 代表一个数据库表中的一行数据，它的定义是一个字典，其中 Key 是列名，Value 是列值。

```csharp
public class BucketItem
{
    public Dictionary<string, BucketItemPropertyValue> Properties { get; set; } = new();
}
```

具体的列值类型是 BucketItemPropertyValue，它的定义如下：

```csharp
public class BucketItemPropertyValue
{
    public object? Value { get; set; }
    public BucketItemPropertyType Type { get; set; }
}
```

显然，BucketItemPropertyValue 代表一个数据库表中的一个单元格，它的 Value 是单元格的值，Type 是单元格的类型。具体类型定义如下：

```csharp
public enum BucketItemPropertyType
{
    Int32,
    Boolean,
    String,
    DateTime,
    Int64,
    Single,
    Double,
    TimeSpan,
    Guid,
    FixedSizeByteArray
}
```

ArrayDb 不支持其他类型。所以，ArrayQL 也不支持其他类型。

既然它和 C# 非常接近，而我的查询后端已经写好了，那么完成上面的工作，只需要借助一个 C# 编译器的前端，我们深度裁剪定制它，让它只支持我们需要的特性，就可以了。

所以，我决定使用 Roslyn 来完成这个工作。Roslyn 是一个 C# 编译器的前端，它可以将 C# 代码解析成语法树，然后我们可以对这个语法树进行修改，最后再将这个语法树转换成 C# 代码。

你不需要提供任何额外的内容，包括：承载为一个 TCP 服务器、客户端代码、SDK代码等。这些不需要。这都是体力活。你应该专注于核心的困难的部分：ArrayQL 的语法解析器。

但是，这项工作显然也并不简单。我需要你在这个工作中帮助我。首先，我写了一个类：这个类可以帮我将用户输入的 ArrayQL 语句解析成什么东西，让我可以执行它。我执行的时候，会输入一个 IDynamicObjectBucket，作为查询的数据源，输出一个 IEnumerable<P>，作为查询的结果。

这段代码我很快就写好了。如下：

```csharp
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;

namespace Aiursoft.ArrayDb.ArrayQl;

public class ArrayQlParser
{
    // Cache compiled queries for better performance
    private readonly Dictionary<string, Func<IDynamicObjectBucket, object>> _compiledQueries = new();

    /// <summary>
    /// Executes an ArrayQL query against the provided bucket
    /// </summary>
    /// <param name="query">The ArrayQL query string</param>
    /// <param name="bucket">The data source</param>
    /// <returns>Query results as an enumerable collection</returns>
    public IEnumerable<dynamic> Run(string query, IDynamicObjectBucket bucket)
    {
        // Get or compile the query
        if (!_compiledQueries.TryGetValue(query, out var compiledQuery))
        {
            compiledQuery = CompileQuery(query);
            _compiledQueries[query] = compiledQuery;
        }

        // Execute the query and process the result
        var result = compiledQuery(bucket);

        // Handle different return types
        if (result is IEnumerable<dynamic> dynamicEnumerable)
        {
            return dynamicEnumerable;
        }
        if (result is IEnumerable<object> objectEnumerable)
        {
            return objectEnumerable;
        }

        // Convert single result to collection
        return [result];
    }

    private Func<IDynamicObjectBucket, object> CompileQuery(string query)
    {
        // Validate the query for safety
        ValidateQuery(query);

        // Create code that will execute the query
        string code = $@"
using System;
using System.Collections.Generic;
using System.Linq;
using Aiursoft.ArrayDb.ObjectBucket.Abstractions.Interfaces;
using Aiursoft.ArrayDb.ObjectBucket.Dynamic;

namespace ArrayQl.Dynamic
{{
    public static class QueryExecutor
    {{
        public static object Execute(IDynamicObjectBucket bucket)
        {{
            var source = bucket.AsEnumerable();
            return {query};
        }}
    }}
}}";

        // Compile the code
        var syntaxTree = CSharpSyntaxTree.ParseText(code);
        MetadataReference[] references =
        [
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IEnumerable<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(IDynamicObjectBucket).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(DynamicObjectBucket).Assembly.Location),
            MetadataReference.CreateFromFile(Assembly.GetExecutingAssembly().Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Runtime").Location),
            MetadataReference.CreateFromFile(Assembly.Load("System.Collections").Location),
        ];

        var compilation = CSharpCompilation.Create("ArrayQl.Dynamic")
            .WithOptions(new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary))
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        using var ms = new MemoryStream();
        var result = compilation.Emit(ms);

        if (!result.Success)
        {
            var errors = result.Diagnostics
                .Where(d => d.Severity == DiagnosticSeverity.Error)
                .Select(d => d.GetMessage());
            throw new InvalidOperationException(
                $"Query compilation failed: {string.Join(Environment.NewLine, errors)}");
        }

        // Load the assembly and create a delegate
        ms.Seek(0, SeekOrigin.Begin);
        var assembly = Assembly.Load(ms.ToArray());
        var type = assembly.GetType("ArrayQl.Dynamic.QueryExecutor");
        var method = type!.GetMethod("Execute");

        return bucket => method!.Invoke(null, [ bucket ])!;
    }

    private void ValidateQuery(string query)
    {
        // Parse the query into a syntax tree
        SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(query);
        CompilationUnitSyntax root = syntaxTree.GetCompilationUnitRoot();

        // Use a syntax visitor to check for unsafe operations
        var visitor = new UnsafeOperationVisitor();
        visitor.Visit(root);

        if (visitor.HasUnsafeOperations)
        {
            throw new InvalidOperationException("The query contains unsafe operations");
        }
    }

    /// <summary>
    /// Syntax walker that checks for unsafe operations in the query
    /// </summary>
    private class UnsafeOperationVisitor : CSharpSyntaxWalker
    {
        private static readonly HashSet<string> AllowedMethods =
        [
            "Where", "Select", "GroupBy", "OrderBy", "OrderByDescending",
            "ThenBy", "ThenByDescending", "Skip", "Take", "Count", "Sum",
            "Min", "Max", "Average", "Any", "All", "First", "FirstOrDefault",
            "Last", "LastOrDefault", "Single", "SingleOrDefault", "ToArray",
            "ToList", "Distinct"
        ];

        public bool HasUnsafeOperations { get; private set; }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            // Check if method is allowed
            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                string methodName = memberAccess.Name.Identifier.Text;
                if (!AllowedMethods.Contains(methodName))
                {
                    HasUnsafeOperations = true;
                }
            }

            base.VisitInvocationExpression(node);
        }

        public override void VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            // Only allow anonymous objects
            if (!node.Type.ToString().Equals("var") &&
                !node.Type.ToString().Equals("anonymous") &&
                !node.Type.ToString().StartsWith("AnonymousType"))
            {
                HasUnsafeOperations = true;
            }

            base.VisitObjectCreationExpression(node);
        }

        // Block other unsafe operations
        public override void VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            HasUnsafeOperations = true;
            base.VisitMethodDeclaration(node);
        }

        public override void VisitClassDeclaration(ClassDeclarationSyntax node)
        {
            HasUnsafeOperations = true;
            base.VisitClassDeclaration(node);
        }

        public override void VisitAssignmentExpression(AssignmentExpressionSyntax node)
        {
            // Disallow assignments except in lambda expressions
            if (!(node.Parent is LambdaExpressionSyntax))
            {
                HasUnsafeOperations = true;
            }

            base.VisitAssignmentExpression(node);
        }
    }
}
```

现在问题时，如果我需要使用它，我不得不这么写：

```csharp
public void TestQuery()
{
    var typeDefine = new BucketItemTypeDefinition
    {
        Properties = new Dictionary<string, BucketItemPropertyType>
        {
            { "MyNumber1", BucketItemPropertyType.Int32 },
            { "MyString1", BucketItemPropertyType.String },
            { "MyNumber2", BucketItemPropertyType.Int32 },
            { "MyBoolean1", BucketItemPropertyType.Boolean },
            { "MyString2", BucketItemPropertyType.String },
            { "MyFixedByteArray", BucketItemPropertyType.FixedSizeByteArray }
        },
        FixedByteArrayLengths = new Dictionary<string, int>
        {
            { "MyFixedByteArray", 30 }
        }
    };
    var persistService = new DynamicObjectBucket(typeDefine, TestFilePath, TestFilePathStrings);

    for (var i = 0; i < 100; i++)
    {
        persistService.Add(new BucketItem
        {
            Properties = new Dictionary<string, BucketItemPropertyValue>
            {
                {
                    "MyNumber1",
                    new BucketItemPropertyValue
                    {
                        Type = BucketItemPropertyType.Int32,
                        Value = i
                    }
                },
                {
                    "MyString1",
                    new BucketItemPropertyValue
                    {
                        Type = BucketItemPropertyType.String,
                        Value = $"Hello, World! 你好世界 {i}"
                    }
                },
                {
                    "MyNumber2",
                    new BucketItemPropertyValue
                    {
                        Type = BucketItemPropertyType.Int32,
                        Value = i * 10
                    }
                },
                {
                    "MyBoolean1",
                    new BucketItemPropertyValue
                    {
                        Type = BucketItemPropertyType.Boolean,
                        Value = i % 2 == 0
                    }
                },
                {
                    "MyString2",
                    new BucketItemPropertyValue
                    {
                        Type = BucketItemPropertyType.String,
                        Value = $"This is another longer string. {i}"
                    }
                },
                {
                    "MyFixedByteArray",
                    new BucketItemPropertyValue
                    {
                        Type = BucketItemPropertyType.FixedSizeByteArray,
                        Value = Encoding.UTF8.GetBytes($"FixedByteArray {i}")
                    }
                }
            }
        });
    }

    // Create parser
    var parser = new ArrayQlParser();

    // Execute a query
    var results = parser.Run("""
                            source
                            .Where(c => (int)c.Properties["MyNumber1"].Value % 2 == 0)
                            """, persistService);

    Assert.AreEqual(50, results.Count());

    // Execute a query
    var resultsCount = parser.Run("""
                            source
                            .Where(c => (int)c.Properties["MyNumber1"].Value % 2 == 0)
                            .Count()
                            """, persistService);

    Assert.AreEqual(50, resultsCount.First() as int?);
}
```

这语法太丑了。我非常想写出：

```csharp
source
.Where(C => c.Value % 2 == 0)
```

请你告诉我代码怎么写，我会将它整合到我的代码中。我需要你的帮助。
