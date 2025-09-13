# 4mc DataSource V2 for Spark 3.2.1

This project is an end‑to‑end example of implementing a custom file format reader for Apache Spark using the **DataSource V2** API.  It demonstrates how to integrate a non‑native compression format (the [4mc format](https://github.com/fingltd/4mc)) with Spark’s distributed file discovery and partition pruning mechanisms while respecting the internal block structure of the compressed files.

The goal of this README is to help a future engineer or AI assistant understand the design choices and recreate the implementation from scratch without copying large code fragments.  It explains the high‑level architecture, the purpose of each class, and highlights important caveats to be aware of when working with Spark 3.2.1, Scala 2.12 and the Janino compiler.

## Background

The 4mc format is a variant of LZ4 compression that stores data in blocks and appends a footer containing a block index.  When reading a file in parallel, it is critical to start each task at the beginning of a block and to stop before entering the next block; otherwise the decompressor will encounter invalid headers.  Hadoop’s `FourMcInputFormat` contains custom logic (`alignSliceStartToIndex`/`alignSliceEndToIndex`) to align splits on block boundaries.  A traditional Spark integration using `sparkContext.newAPIHadoopFile` delegates splitting to Hadoop and never benefits from Spark’s own file indexing and coalescing capabilities.

To avoid those limitations, this project uses **FileDataSourceV2** and **FileTable** to obtain distributed file listing and partition pruning from Spark, and then applies 4mc‑specific split logic only after Spark has planned the initial `PartitionedFile`s.

## High‑Level Architecture

The implementation consists of a handful of small classes, each fulfilling a specific role in the DataSource V2 stack:

| Component | Responsibility |
| --- | --- |
| **`FourMcFileDataSource`** | Entry point for Spark.  Implements `FileDataSourceV2` and `DataSourceRegister`, providing the short name `"fourmc"`.  It parses `path`/`paths` options, strips them from the options map, and returns a `FourMcTable`. |
| **`FourMcTable`** | Extends `FileTable` to inherit Spark’s distributed file listing via `PartitioningAwareFileIndex`.  It constructs a `FourMcScanBuilder` which retains knowledge of the file index and options.  Schema inference simply defers to user input, since the data is plain text. |
| **`FourMcScanBuilder`** | Decides the logical schema based on the `withOffset` option (either a single `STRING` column named `value` or two columns `offset`/`value`).  It retrieves the partition schema from the file index and returns a new `FourMcScan` with empty filter lists. |
| **`FourMcScan`** | Extends `FileScan` and implements Spark’s `Batch` interface.  It stores `sparkSession`, a `PartitioningAwareFileIndex`, the read data and partition schemas, and lists of partition/data filters (although filter pushdown is not implemented).  The key method is `planInputPartitions()`: it calls `super.planInputPartitions()` to get Spark’s default `FilePartition`s, then expands each `PartitionedFile` into block‑aligned slices using `FourMcBlockPlanner`.  Expanded slices are regrouped into `FilePartition`s to avoid tiny tasks.  `withFilters()` returns a copy of the scan with updated filters. |
| **`FourMcBlockPlanner`** | Reads the 4mc footer index using `FourMcBlockIndex.readIndex(FileSystem, Path)`.  It computes absolute block start offsets and groups contiguous blocks into slices no larger than `maxPartitionBytes` (default is `spark.sql.files.maxPartitionBytes`).  The first slice always starts at byte `0` to avoid skipping the first line, and the final slice’s end offset is set to the file length to ensure the last line is read. |
| **`FourMcPartitionReaderFactory`**, **`FourMcMultiSliceReader`** and **`FourMcSliceReader`** | These classes implement the reader side.  A `FourMcMultiSliceReader` iterates over one or more block‑aligned slices in a `FilePartition`, opening a new `FourMcSliceReader` for each slice.  A `FourMcSliceReader` creates a `FourMcLineRecordReader` on a `FileSplit(start, length)` and emits each line as an `InternalRow` (and its byte offset if `withOffset=true`).  Only the type‑erased `createReader()` method is marked with `@Override` to satisfy the Janino compiler. |

Together, these components allow Spark to read `.4mc` files via

```scala
spark.read
  .format("fourmc")
  .option("path", "/path/to/file.4mc")
  .option("withOffset", "true")
  .load()
```

## Implementation Notes

* **Use of `PartitioningAwareFileIndex`**: By extending `FileTable`, the code obtains a `PartitioningAwareFileIndex` which performs a distributed directory listing and caches file statuses.  This index also exposes a partition schema that Spark uses for partition pruning.  The scan builder captures this schema so it can be passed into the `FileScan` constructor.

* **Block‑aligned splitting**: The 4mc footer stores relative block offsets.  `FourMcBlockPlanner` reads the footer, converts relative offsets to absolute byte positions, and then slices the file accordingly.  The `maxPartitionBytes` option controls how many compressed bytes are grouped into a single slice; this can reduce the number of tasks on clusters with many small blocks.

* **First and last slices**: When a `FourMcLineRecordReader` is given a non‑zero start offset, it drops the first line to avoid returning a partial record.  To prevent losing the very first record, the first slice always begins at offset 0.  Similarly, Hadoop’s default behaviour stops reading at the requested end offset; to ensure the final line is included, the last slice extends to the end of the file.

* **Broadcasting Hadoop configuration**: Spark executors cannot deserialize a raw `org.apache.hadoop.conf.Configuration`.  Use `org.apache.spark.util.SerializableConfiguration` and broadcast it once from the driver.  Each reader task then reconstructs a `Configuration` using `broadcastConf.value.value`.

* **Janino and generics**: The built‑in Janino compiler (used by Spark for code generation) does not generate bridge methods for generic interfaces.  To avoid `NoSuchMethodError`, only the type‑erased `createReader(InputPartition)` method of `PartitionReaderFactory` is annotated with `@Override`.  Do not override the generic variants.

* **Dependency management**: The `build.sbt` file declares Spark 3.2.1 and Scala 2.12.x as `provided` dependencies and includes your chosen version of the 4mc Hadoop library as a `compile` or `runtime` dependency.  The `javax.management` module must also be provided to satisfy Spark’s requirements.  You must place the 4mc JAR on the driver and executor classpaths when submitting a job (e.g., via `spark-submit --packages com.github.fingltd:hadoop-4mc:<version>` or `--jars`).

## Building and Running

1. **Check prerequisites**: Install SBT 1.11.3 and a JDK compatible with Scala 2.12.  Ensure you have access to the 4mc Hadoop JAR.

2. **Compile**: Run `sbt package` from the project root.  This will produce a JAR under `target/scala-2.12/`.  No code generation is required at compile time.

3. **Submit to Spark**: When running your Spark job, include the compiled JAR and the 4mc dependency on the classpath.  For example:

   ```bash
   spark-submit \
     --class com.example.Main \
     --conf spark.sql.files.maxPartitionBytes=134217728 \
     --jars /path/to/hadoop-4mc.jar,/path/to/fourmc-datasource-v2.jar \
     your-application.jar
   ```

4. **Use the DataSource**: In your Spark code, read `.4mc` files just as you would any built‑in format:

   ```scala
   val df = spark.read
     .format("fourmc")
     .option("path", "/data/logs/*.4mc")
     .option("withOffset", "false")
     .load()

   df.show(false)
   ```

## Caveats and Future Work

* **No filter pushdown**: Partition filters and data filters are preserved but not applied at the reader level.  All rows are read before Spark applies predicates.  Implementing block‑level filter pushdown would require parsing the contents of the blocks or supporting additional statistics.

* **Single record type**: The current reader treats each line as a single string, optionally with its byte offset.  To support semi‑structured formats (CSV, JSON) inside 4mc files, you would need to wrap the output of this DataSource with Spark’s built‑in CSV/JSON readers or implement a custom `PartitionReader` that parses the lines.

* **Block size and skew**: 4mc blocks are compressed chunks; uncompressed sizes may vary significantly.  When `maxPartitionBytes` is too small relative to block sizes, tasks may process only one block.  Conversely, extremely large blocks may still produce imbalanced partitions.  There is no direct way to override the block size of existing files.

* **Compatibility**: This implementation targets Spark 3.2.1 and Scala 2.12.x.  Newer Spark versions may have different APIs for DataSource V2 and may require adaptations.

## Recreating the Project

To reproduce this project from scratch:

1. **Set up an SBT project** with Spark 3.2.1 and Scala 2.12.  Declare the 4mc Hadoop library as a dependency and mark Spark as a `provided` dependency.

2. **Implement a `FileDataSourceV2`** that registers a short name (`fourmc`) and returns a custom `FileTable` when `getTable(...)` is called.  Parse `path`/`paths` options and strip them from the remaining options map.

3. **Extend `FileTable`** to create a table class that accepts a `SparkSession`, a `CaseInsensitiveStringMap` of options, a list of input paths, and an optional user schema.  Override `newScanBuilder()` to construct your scan builder.  You can defer schema inference to the user since the reader emits plain text.

4. **Create a `ScanBuilder`** that captures the `PartitioningAwareFileIndex` provided by `FileTable` and the options.  When `build()` is called, decide whether to include the offset column based on the options, compute a `StructType` accordingly, fetch the partition schema from the file index, and construct a `FileScan` implementation.

5. **Implement a `FileScan`** subclass that stores `sparkSession`, the file index, the read data schema, the read partition schema, and filter lists.  Override `planInputPartitions()` to call the parent’s method to obtain Spark’s default partitions, then expand each `PartitionedFile` into block‑aligned slices using a helper object.  Override `createReaderFactory()` to supply a reader factory, and implement `withFilters()` to return a new instance with updated filters.

6. **Read the 4mc block index** in your helper object (`FourMcBlockPlanner`).  Use the block index to compute a list of block start positions and generate slices that satisfy `maxPartitionBytes`.  Always include the first block in the first slice and end the final slice at the file length.

7. **Implement reader factories and readers**.  A `PartitionReaderFactory` should produce a multi‑slice reader for each `FilePartition`.  Each slice reader should wrap a `FourMcLineRecordReader` (from the 4mc Hadoop library) in a `FileSplit`.  Use `SerializableConfiguration` to broadcast Hadoop configuration.  Only override the type‑erased `createReader(InputPartition)` method.

8. **Register the DataSource** via `META-INF/services` entries for `org.apache.spark.sql.connector.catalog.TableProvider` and `org.apache.spark.sql.sources.DataSourceRegister`.  Both files should contain the fully‑qualified class name of your `FileDataSourceV2` implementation.

9. **Package and test**.  Build your project with SBT, add the JAR and the 4mc dependency to Spark’s classpath, and verify that `spark.read.format("fourmc")` correctly loads your compressed files.  Check corner cases such as files with only one block, multiple input paths, and the presence or absence of a trailing newline on the last line.

Following these steps and design notes should enable an engineer or AI system to reproduce the functionality without reference to the original source code.