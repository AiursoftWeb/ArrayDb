# Aiursoft ArrayDb

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://gitlab.aiursoft.com/aiursoft/arraydb/-/blob/master/LICENSE)
[![Pipeline stat](https://gitlab.aiursoft.com/aiursoft/arraydb/badges/master/pipeline.svg)](https://gitlab.aiursoft.com/aiursoft/arraydb/-/pipelines)
[![Test Coverage](https://gitlab.aiursoft.com/aiursoft/arraydb/badges/master/coverage.svg)](https://gitlab.aiursoft.com/aiursoft/arraydb/-/pipelines)
[![NuGet version](https://img.shields.io/nuget/v/Aiursoft.ArrayDb.Partitions.svg?style=flat-square)](https://www.nuget.org/packages/Aiursoft.ArrayDb.Partitions/)
[![Man hours](https://manhours.aiursoft.com/r/gitlab.aiursoft.com/aiursoft/arraydb.svg)](https://manhours.aiursoft.com/r/gitlab.aiursoft.com/aiursoft/arraydb.html)

Aiursoft ArrayDb is a lightweight, efficient database engine optimized for storing fixed-length data with constant-time indexing performance (O(1)). ArrayDb is ideal for scenarios where fast, reliable storage and access to time-sequenced or resource-utilization data are essential, making it a strong choice for logging, telemetry, and performance tracking use cases.

## Key Design Principles of ArrayDb

Unlike traditional databases, which can struggle with high-frequency data storage requirements, ArrayDb is purpose-built for append-only, fixed-length data storage. It stores entries as continuous, fixed-length data blocks, optimized for minimal read/write operations, ensuring high performance on both SSDs and HDDs.

ArrayDb organizes data into two categories:

1. **Fixed-Length Attributes**: These attributes, such as integers, `DateTime`, and booleans, are stored in a fixed-length array, facilitating fast O(1) access by index.
2. **Variable-Length Attributes**: For data like strings, ArrayDb maintains a separate variable-length array, where each entry contains a pointer in the fixed-length array for fast access.

### Advantages of ArrayDb

1. **High-Speed Access**: ArrayDb stores each element sequentially on disk. By calculating an element’s exact location based on its index, ArrayDb can retrieve or count elements in constant time, O(1).
2. **Optimized Append Performance**: New entries are simply appended, which maintains data consistency and reduces disk fragmentation. This approach also ensures sustained high-speed write performance, even under heavy load.
3. **Efficient Reads**: Reading from ArrayDb only requires a single disk operation to load all columns in an entry, which contrasts with traditional columnar storage that performs multiple reads for each column.

### Limitations

ArrayDb is designed for simplicity and speed but with limited data manipulation:

- **No Structural Modifications**: The structure of stored data cannot be edited after creation.
- **Append-Only**: Supports only appending new entries; no item deletions or mid-array insertions are allowed.
- **Limited Updates**: Modifying variable-length data (e.g., resizing strings) is not supported.

### Best Use Cases for ArrayDb

- **Fixed-Length Data**: Suitable for storing time-series data, such as CPU or memory usage metrics.
- **Time-Based Indexing**: Ideal for sequential logging or telemetry data, where entries are naturally appended and queried by timestamp.

### Read-Write Performance Difference

Large-scale writes are significantly faster than reads because ArrayDb optimizes writes by pre-arranging data (including strings) in memory. This enables sequential, continuous writes to disk, minimizing disk-seeking time to O(1).

In contrast, reads require accessing each string or variable-length attribute individually, creating random access patterns due to potential data fragmentation. As a result, reading incurs a higher O(n) disk-seeking time, where n is the element count. ArrayDb uses an LRU cache to reduce physical disk reads, but in multi-threaded reads, this cache introduces high CPU load.

### Project structure

* The FilePersists provides a service to read and write in actual files.
* The ReadLruCache provides a service to cache the read data, while keeping the API same with FilePersists.
* The StringRepository provides a service to manage the string data.
* The ObjectBucket provides a service to manage the object data, that can save the object array on disk.
* The WriteBuffer is a decorated ObjectBucket that can buffer the write operation to improve the write performance. However, it costs additional read time because it may lock the read when writing.
* The Partitions is a decorated ObjectBucket that can partition the data by a partition key. It can improve the read performance when you need to read data from a specific partition.

```mermaid
---
title: Project dependency diagram
---

stateDiagram-v2
    ReadLruCache --> FilePersists
    StringRepository --> ReadLruCache
    ObjectBucket.Dynamic --> StringRepository
    WriteBuffer.Dynamic --> ObjectBucket.Dynamic
    ObjectBucket --> ObjectBucket.Dynamic
    WriteBuffer --> ObjectBucket
    Partitions --> WriteBuffer
    Benchmark --> WriteBuffer
    Tests --> Partitions
    Tests --> WriteBuffer.Dynamic
```

For most cases, it's suggested to directly use the `Partitions` module. It provides the best performance and the most features.

If your case is simple and you don't need partition, you can use the `BufferedBucket` module. It provides the best write performance. However, if you don't need the write performance, you can use the `ObjectBucket` module.

```bash
dotnet add package Aiursoft.ArrayDb.Partitions
dotnet add package Aiursoft.ArrayDb.WriteBuffer
dotnet add package Aiursoft.ArrayDb.ObjectBucket
```

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

## Performance Test Report

ArrayDb has shown significant performance improvements. With a buffer, it can insert 1M items in 20.24ms and read 1M items in 12.72ms.

Without a buffer, it can insert 1M items within 301.28ms and read 1M items within 500.19ms.

Here is the updated performance test report.

### Test platform

* **CPU**: 13th Gen Intel(R) Core(TM) i9-13900KS  
* **RAM**: 32GB DDR5 6400MHz  
* **Disk**: Samsung 990 PRO 1TB NVMe SSD  
* **OS**: AnduinOS 1.0.3  
* **File system**: ext4  
* **.NET**: 8.0.110, Release build, Linux-x64  

Each test case includes a warm-up phase (2 runs) followed by the actual test (3 runs), with the average time recorded.

### Performance Data

| Test Case                             | Bucket                               | Buffered Bucket                    | Buffered Buffered Bucket           | Buffered Buffered Buffered Bucket  |
|---------------------------------------|--------------------------------------|------------------------------------|------------------------------------|------------------------------------|
| Add 1 time with 1M items              | 301.2844 ms (S),                     | 51.1672 ms (S),                    | 48.0842 ms (S),                    | 20.2399 ms (S),                    |
| Add 1K items 1K times                 | 156.2232 ms (S), 360.7387 ms (P)     | 11.5777 ms (S), 18.6601 ms (P)     | 11.0119 ms (S), 15.0955 ms (P)     | 11.2678 ms (S), 17.3086 ms (P)     |
| Add 1M times with 1 item              | 14889.9691 ms (S), 60107.1009 ms (P) | 117.4008 ms (S), 252.2507 ms (P)   | 43.686 ms (S), 320.6695 ms (P)     | 62.8653 ms (S), 324.4333 ms (P)    |
| Read 1 time with 1M items             | 500.1882 ms (S),                     | 15.2656 ms (S),                    | 12.7197 ms (S),                    | 685.4688 ms (S),                   |
| Read 1K items 1K times                | 146.3647 ms (S), 460.1882 ms (P)     | 508.0884 ms (S), 758.9298 ms (P)   | 534.3185 ms (S), 762.8016 ms (P)   | 584.8098 ms (S), 745.3977 ms (P)   |
| Read 1 item 1M times                  | 166.9941 ms (S), 146.3064 ms (P)     | 499.8569 ms (S), 676.1447 ms (P)   | 582.3701 ms (S), 922.2429 ms (P)   | 648.5719 ms (S), 1223.5448 ms (P)  |
| Write 7 read 3 1000 items, 1000 times | 143.0727 ms (S), 379.7132 ms (P)     | 227.178 ms (S), 138.937 ms (P)     | 200.0433 ms (S), 195.5767 ms (P)   | 295.2009 ms (S), 162.815 ms (P)    |
| Write 3 read 7 1000 items, 1000 times | 134.575 ms (S), 383.6223 ms (P)      | 147.3032 ms (S), 113.7026 ms (P)   | 170.7177 ms (S), 82.2303 ms (P)    | 244.2066 ms (S), 54.9889 ms (P)    |

In the table:

* S means single-threaded, like: `for (int i = 0; i < 1000; i++) { db.Add(new MyLogItem()); }`
* P means multi-threaded, like: `Parallel.For(0, 1000, i => { db.Add(new MyLogItem()); });`

## How to contribute

There are many ways to contribute to the project: logging bugs, submitting pull requests, reporting issues, and creating suggestions.

Even if you with push rights on the repository, you should create a personal fork and create feature branches there when you need them. This keeps the main repository clean and your workflow cruft out of sight.

We're also interested in your feedback on the future of this project. You can submit a suggestion or feature request through the issue tracker. To make this process more effective, we're asking that these include more information to help define them more clearly.
