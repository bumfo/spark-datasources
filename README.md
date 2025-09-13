# fourmc-datasource-v2: Spark DataFrame integration for 4mc with DSv2

This project provides a **DataSource V2** implementation for reading 4mc‐compressed text files into Spark DataFrames.  It extends Spark’s **FileDataSourceV2** and **FileTable** to leverage Spark’s distributed file discovery and partition pruning.  Unlike earlier attempts that relied on `newAPIHadoopFile` or bridge formats, this implementation honors the 4mc block index when planning splits and uses Spark’s native DataFrame API end‑to‑end.

## Key features

- **Block‑aligned splitting**: Uses the 4mc footer index (via `FourMcBlockIndex`) to align partition boundaries to compressed block starts and ends【696825297841155†L145-L188】.  The first partition always starts at byte `0L`, so the `FourMcLineRecordReader` does not skip the first line【770294647968254†L152-L158】, and the final partition always extends to the end of the file.  This avoids decoding in the middle of a block and prevents errors like “compressed length exceeds max block size”.
- **DataSource V2 integration**: Extends `FileDataSourceV2`, `FileTable` and `FileScan` so you can call `spark.read.format("fourmc")` directly—no `newAPIHadoopFile` intermediate required.  By building on Spark’s file‑source stack, the implementation benefits from distributed file listing, partition pruning and automatic coalescing.
- **Options**:
    - `path` or `paths`: one or multiple comma‐separated paths to files/directories.
    - `withOffset` (default `false`): when `true`, includes a `BIGINT offset` column with the byte position of each line.
    - `encoding` (default `UTF-8`): character set used to decode lines.
    - `maxPartitionBytes` (default `spark.sql.files.maxPartitionBytes` or `128MiB`): maximum compressed bytes per partition.
    - Arbitrary `conf.*` options can be passed through to the Hadoop `Configuration` if needed (e.g., to adjust Hadoop FS settings).
- **Janino‐safe**: Only type‐erased interfaces are overridden; there is no generic method signature that Janino would struggle to bridge.

## How splitting works

1. For each file, the code reads the 4mc block index using `FourMcBlockIndex.readIndex(...)`【696825297841155†L145-L188】.
2. It computes absolute block offsets (compressed positions) and accumulates them into **block‑aligned slices** whose total length does not exceed `maxPartitionBytes`.  The first slice starts at `0L`; the last slice ends at the file length.
3. These slices are mapped back into Spark’s generic `PartitionedFile` abstraction, so Spark can coalesce them into `FilePartition`s for scheduling.
4. At read time, the `FourMcPartitionReader` instantiates a `FourMcLineRecordReader` on the corresponding `FileSplit(start, length)` and yields lines (and offsets, if requested) as `InternalRow`s.

## Usage

1. Build the project:

```bash
sbt package
```

2. Ship the resulting JAR to your Spark cluster.

3. Register and read:

```scala
spark.read
  .format("fourmc")
  .option("path", "/path/to/your/file.4mc")
  .option("withOffset", "true")    // optional
  .option("maxPartitionBytes", "67108864")  // optional (64 MiB)
  .load()
  .show(false)
```

You can specify multiple files or directories in `paths` (comma‐separated). The reader will recursively descend into directories and ignore hidden files (`.` and `_` prefixes) and non‐4mc files.

## Limitations & future work

- Currently only supports **text lines** with optional offsets. If you need to emit raw bytes or support other formats (CSV, JSON), extend the `FourMcPartitionReader` accordingly.
- The block‐aligned partitioning uses compressed byte sizes; large uncompressed blocks may skew data distribution.
- Schema pruning and filter pushdown are not implemented. Those would require further integration with Spark’s predicate pushdown API.

## References

- 4mc block index usage and alignment functions (`alignSliceStartToIndex`, `alignSliceEndToIndex`) are described in the source【696825297841155†L145-L188】.
