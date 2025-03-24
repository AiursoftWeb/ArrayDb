下面的英文文档，是我之前开发的 ArrayDb 的文档。

ArrayDb 是一个数据库。但是，很遗憾的是，不同于 MySQL 它是个进程，ArrayDb 目前还是个库。它的用法如下：

=====================================

## How to use ArrayDb

Before starting, you need to install [.NET 9 SDK](https://dot.net) on your machine.

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

```aql
MyTable
.Skip(10)  
.Take(20)
```

上面的语句表示从 MyTable 表中跳过 10 条数据，然后取 20 条数据。

我会提供更多示例，例如：

查询我的所有 Customer 表中，资产大于 10 亿美元的美国客户或者年龄为 18 岁的非程序员客户的数量。

```aql
Customers
.Where(c => c.Assets > 1000000000 && c.Country == "USA" || c.Age == 18 && c.Occupation != "Programmer")
.Count()
```

查询我的所有 Customer 表中，按照国家分组，统计每个国家的客户数量。

```aql
Customers
.GroupBy(c => c.Country)
.Select(c => new { Country = c.Key, Count = c.Count() })
```

查询我的所有 HTTP 请求中，最近3天的，所有向 Nextcloud 的请求里，每天成功和失败的请求数量。

```aql
HttpRequests
.Where(r => r.RequestTime > DateTime.Now.AddDays(-3))
.Where(r => r.Target == "Nextcloud")
.GroupBy(r => r.RequestTime.Date)
.Select(r => new { Date = r.Key, Success = r.Count(r => r.StatusCode == 200), Failed = r.Count(r => r.StatusCode != 200) })
```

看得出来，这个语言和 C# 的 LINQ 很像。但是，ArrayQL 是一个查询语言，不是一个编程语言。它只支持查询，不支持修改。它不支持变量，不支持循环，不支持递归，不支持异常处理。它虽然支持 Lambda 表达式，但是只支持一部分 C# 的 Lambda 表达式。

所以，为了完成上面的工作，我决定这么设计它的语法解析器。

首先，语法解析器一定是一个类，它的核心函数：Run，需要输入一个 string，作为 ArrayQL 的查询语句，输入一个 IEnumerable<T>，作为查询的数据源，输出一个 IEnumerable<P>，作为查询的结果。

其中，T是我已经定义好的数据库实体类，P是一个匿名类，用于表示查询的结果。

T 只有下面几种类型：

* int
* bool
* string
* DateTime
* long
* float
* double
* TimeSpan
* Guid
* byte[]

ArrayDb 不支持其他类型。所以，ArrayQL 也不支持其他类型。

既然它和 C# 非常接近，而我的查询后端已经写好了，那么完成上面的工作，只需要借助一个 C# 编译器的前端，我们深度裁剪定制它，让它只支持我们需要的特性，就可以了。

所以，我决定使用 Roslyn 来完成这个工作。Roslyn 是一个 C# 编译器的前端，它可以将 C# 代码解析成语法树，然后我们可以对这个语法树进行修改，最后再将这个语法树转换成 C# 代码。

但是，这项工作显然也并不简单。我需要你在这个工作中帮助我。
