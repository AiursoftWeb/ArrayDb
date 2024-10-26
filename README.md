# Aiursoft ArrayDb

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://gitlab.aiursoft.cn/aiursoft/arraydb/-/blob/master/LICENSE)
[![Pipeline stat](https://gitlab.aiursoft.cn/aiursoft/arraydb/badges/master/pipeline.svg)](https://gitlab.aiursoft.cn/aiursoft/arraydb/-/pipelines)
[![Test Coverage](https://gitlab.aiursoft.cn/aiursoft/arraydb/badges/master/coverage.svg)](https://gitlab.aiursoft.cn/aiursoft/arraydb/-/pipelines)
[![ManHours](https://manhours.aiursoft.cn/r/gitlab.aiursoft.cn/aiursoft/arraydb.svg)](https://gitlab.aiursoft.cn/aiursoft/arraydb/-/commits/master?ref_type=heads)

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

## How to contribute

There are many ways to contribute to the project: logging bugs, submitting pull requests, reporting issues, and creating suggestions.

Even if you with push rights on the repository, you should create a personal fork and create feature branches there when you need them. This keeps the main repository clean and your workflow cruft out of sight.

We're also interested in your feedback on the future of this project. You can submit a suggestion or feature request through the issue tracker. To make this process more effective, we're asking that these include more information to help define them more clearly.
