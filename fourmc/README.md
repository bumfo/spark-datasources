# FourMC DataSource V2 for Spark 3.2.1

The `fourmc` module exposes a Spark **DataSource V2** reader that understands
the [4mc](https://github.com/fingltd/4mc) block format.  It plans splits on
compressed block boundaries, so Spark can read `.4mc` text files through the
DataFrame API without falling back to `newAPIHadoopFile`.

## Highlights

- **Block-aware planning** – the planner reads the 4mc footer index and emits
  slices that always start and finish on block boundaries, preventing partial
  decompression and “invalid header” errors.
- **File source integration** – the implementation extends Spark’s
  `FileDataSourceV2` stack, so direct calls to `spark.read.format("fourmc")`
  benefit from distributed listing, partition pruning, and automatic task
  coalescing.
- **Configurable reader** – options cover multi-path inputs, optional offset
  emission, character encoding, and a per-task compressed size limit.
  Additional `conf.*` entries pass through to the Hadoop configuration when
  you need filesystem tweaks.

## Build & Package

```bash
sbt package
```

The assembled JAR lands in `target/scala-2.12/`.  Ship the JAR together with
the 4mc Hadoop codec (runtime dependency) when submitting Spark jobs.  Spark’s
own libraries remain `provided`.

## Reading `.4mc` Files

```scala
val df = spark.read
  .format("fourmc")
  .option("paths", "/data/logs/*.4mc")
  .option("withOffset", "true")      // include a BIGINT byte offset column
  .option("maxPartitionBytes", "64MB")
  .load()

df.show(false)
```

Accepted options:

| Option | Description |
| --- | --- |
| `path` / `paths` | Single path or comma-separated list of files/directories.  Directories are scanned recursively; hidden files (`._`, `.`, `_`) are skipped. |
| `withOffset` (default `false`) | When `true`, emits two columns: `offset: BIGINT` and `value: STRING`.  Otherwise only `value` is returned. |
| `encoding` (default `UTF-8`) | Charset used to decode each line. |
| `maxPartitionBytes` | Maximum compressed bytes per planned slice.  Defaults to `spark.sql.files.maxPartitionBytes` (128 MiB unless reconfigured). |
| `conf.*` | Keys prefixed with `conf.` are copied directly into the Hadoop `Configuration`. |

## Architecture Overview

The implementation consists of a few focused classes:

| Component | Purpose |
| --- | --- |
| `FourMcFileDataSource` | Spark entry point.  Registers the `fourmc` short name, parses `path`/`paths`, removes them from the option map, and builds a `FourMcTable`. |
| `FourMcTable` | Extends `FileTable`, retaining Spark’s `PartitioningAwareFileIndex`.  Constructs a scan builder and caches it so repeated reads reuse the same planner. |
| `FourMcSchemaAwareDataSource` and CSV/JSON derivatives | Provide schema-bearing datasources layered on top of the shared planner (see root README for details). |
| `FourMcScanBuilder` | Computes the read schema (with or without offsets) and builds a `FourMcScan` carrying Spark’s partition schema. |
| `FourMcScan` | Wraps Spark’s `FileScan`.  Delegates to the shared planner to expand each `PartitionedFile` into block-aligned slices, then materialises partitions for execution.  Filters are stored for API compatibility but not pushed down. |
| `FourMcPlanner` | Reads the 4mc footer using `FourMcBlockIndex`, converts relative offsets to absolute positions, and groups blocks into slices capped by `maxPartitionBytes`.  The first slice begins at byte `0` and the last ends at the file length. |
| `FourMcPartitionReaderFactory` / `FourMcMultiSliceReader` / `FourMcSliceReader` | Execute the read path.  A multi-slice reader iterates the slices in a `FilePartition`, creating a `FourMcSliceReader` for each.  Each slice reader delegates to `FourMcLineRecordReader` and converts results to `InternalRow`s (offset + UTF8 string). |

## Design Notes & Caveats

- This module currently emits plain-text rows.  Higher-level CSV/JSON support
  lives in the same repository; see `spark-datasources`’s top-level README.
- Partition and data filters are preserved on the `FourMcScan` object but are
  not pushed to the 4mc reader.  Spark evaluates predicates after the rows are
  produced.
- `maxPartitionBytes` is expressed in compressed bytes.  Large variations in
  uncompressed block sizes can still lead to task skew.
- The code targets Spark 3.2.1 on Scala 2.12.  API changes in newer Spark
  releases may require adjustments.

## Rebuilding the Module from Scratch

If you ever need to recreate this reader:

1. Bootstrap an SBT project that depends on Spark 3.2.1 (scope `provided`) and
   the 4mc Hadoop codec.
2. Implement `FileDataSourceV2` + `DataSourceRegister` to expose the short
   name and parse input paths.
3. Extend `FileTable` to capture Spark’s file index, returning a custom scan
   builder that knows about the options and schema.
4. Build a `FileScan` subclass that stores the partition schema, overrides
   `planInputPartitions()` to ask the planner for block slices, and creates a
   reader factory backed by `SerializableConfiguration`.
5. Implement the planner and reader helpers that translate between Spark’s
   `PartitionedFile`s and the 4mc block/line readers.
6. Register the datasource via `META-INF/services` entries for
   `TableProvider` and `DataSourceRegister`.
7. Package, ship, and verify with `spark.read.format("fourmc")` against a
   collection of `.4mc` files.

With these pieces in place, Spark jobs can treat compressed 4mc text as a
first-class file source while staying aligned with built-in DataSource V2
patterns.
