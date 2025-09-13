package org.apache.spark.sql.fourmc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{Batch, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.mutable

/**
 * Builder for 4mc scans.  Similar to CSVScanBuilder, it accepts Spark's
 * internal FileIndex and schemas, then produces a [[FourMcScan]] when
 * build() is invoked.
 */
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex

/**
 * Builder for 4mc scans.  Similar to Spark's CSVScanBuilder, this builder
 * accepts a partitioning-aware file index along with the Spark session and
 * options.  When `build()` is called, it determines whether to include
 * the offset column based on the `withOffset` option and constructs an
 * appropriate read schema.  The builder also fetches the partition
 * schema from the file index so that the resulting scan is aware of
 * partition columns.  Partition and data filters are initialized to empty
 * sequences; Spark will supply them via `withFilters` when necessary.
 */
final class FourMcScanBuilder(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    options: CaseInsensitiveStringMap
) extends ScanBuilder {

  // Determine whether to include the offset column.  We re-compute the data
  // schema accordingly when building the scan.
  private val withOffset: Boolean =
    java.lang.Boolean.parseBoolean(options.getOrDefault("withOffset", "false"))

  import org.apache.spark.sql.types.{LongType, StringType, StructField}

  override def build(): Scan = {
    val resolvedSchema = if (withOffset) {
      // When the offset column is requested, return a two-column schema:
      // (offset LONG, value STRING).
      StructType(Seq(
        StructField("offset", LongType, nullable = false),
        StructField("value", StringType, nullable = true)
      ))
    } else {
      // Single column schema: value STRING.
      StructType(Seq(StructField("value", StringType, nullable = true)))
    }
    val partitionSchema = fileIndex.partitionSchema
    new FourMcScan(
      sparkSession = spark,
      fileIndex = fileIndex,
      readDataSchema = resolvedSchema,
      options = options,
      readPartitionSchema = partitionSchema,
      partitionFilters = Seq.empty,
      dataFilters = Seq.empty
    )
  }
}

/**
 * FileScan for 4mc files.  This class builds upon Spark's [[FileScan]] to
 * utilise distributed file listing and partition planning, but overrides the
 * split logic to align partitions with 4mc block boundaries using the
 * footer index.  It returns a reader factory that instantiates per-slice
 * readers.
 */

/**
 * FileScan for 4mc files.  This class builds upon Spark's [[FileScan]] to
 * utilise distributed file listing and partition planning.  It overrides the
 * split logic to align partitions with 4mc block boundaries using the
 * footer index.  It stores partition and data filters (although 4mc does
 * not currently support filter pushdown) and returns a reader factory
 * that instantiates per-slice readers.  The first slice of every file starts
 * at offset 0L to prevent dropping the first line, and the final slice
 * extends to the end of the file to read the last line.
 */
final class FourMcScan(
    override val sparkSession: SparkSession,
    override val fileIndex: PartitioningAwareFileIndex,
    override val readDataSchema: StructType,
    options: CaseInsensitiveStringMap,
    override val readPartitionSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val dataFilters: Seq[Expression]
) extends FileScan with Batch {

  // Broadcast the Hadoop configuration so that executors can construct
  // FileSystem and FourMcLineRecordReader instances without serializing
  // Configuration directly.  Spark provides SerializableConfiguration for
  // this purpose.
  private val broadcastConf: Broadcast[SerializableConfiguration] =
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(sparkSession.sessionState.newHadoopConf()))

  // Maximum bytes per partition used when expanding 4mc block slices.
  private val maxPartitionBytes: Long = sparkSession.sessionState.conf.filesMaxPartitionBytes

  // Match FileScan's case-sensitivity and name normalization logic
  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
  // Parallel expand configuration (defaults to Spark's discovery settings)
  private val parallelExpandEnabled: Boolean =
    java.lang.Boolean.parseBoolean(options.getOrDefault("fourmc.parallelExpand.enabled", "true"))
  private val parallelExpandThreshold: Int =
    Option(options.get("fourmc.parallelExpand.threshold")).map(_.toInt)
      .getOrElse(sparkSession.sessionState.conf.parallelPartitionDiscoveryThreshold)
  private val parallelExpandMax: Int =
    Option(options.get("fourmc.parallelExpand.maxParallelism")).map(_.toInt)
      .getOrElse(sparkSession.sessionState.conf.parallelPartitionDiscoveryParallelism)

  /**
   * Human-readable description used in the query plan.  This appears in the
   * physical plan and helps users understand that a 4mc-specific scan is
   * occurring.
   */
  override def description: String = "FourMcFileScan"

  /**
   * Convert this scan into a Batch.  Since this scan does not support
   * streaming, the same instance implements both Scan and Batch.
   */
  override def toBatch: Batch = this

  /**
   * Override FileScan.partitions to apply 4mc block-aware expansion before
   * Spark converts partitions into input partitions. We expand each
   * PartitionedFile into one or more block-aligned slices using the 4mc
   * footer index, then coalesce those slices back into FilePartitions using
   * Spark's helper to balance task sizes.
   */
  override def partitions: Seq[FilePartition] = {
    // This inlines FileScan.partitions and replaces file splitting with 4mc-aware expansion.
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = FilePartition.maxSplitBytes(sparkSession, selectedPartitions)

    val partitionAttributes = fileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw org.apache.spark.sql.errors.QueryCompilationErrors
          .cannotFindPartitionColumnInPartitionSchemaError(readField, fileIndex.partitionSchema))
    }
    lazy val partitionValueProject =
      org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
        .generate(readPartitionAttributes, partitionAttributes)

    val allFiles = selectedPartitions.flatMap(_.files)
    val useParallel = parallelExpandEnabled && allFiles.size >= parallelExpandThreshold

    val splitFiles: Seq[PartitionedFile] = if (useParallel) {
      // Map each file path to its partition values
      val partValuesByPath = mutable.HashMap.empty[String, org.apache.spark.sql.catalyst.InternalRow]
      // Cache preferred hosts per file path using Spark's helper
      val hostsByPath = mutable.HashMap.empty[String, Array[String]]
      selectedPartitions.foreach { partition =>
        val pvalues = if (readPartitionAttributes != partitionAttributes) {
          partitionValueProject(partition.values).copy()
        } else partition.values
        partition.files.foreach { f =>
          val pStr = f.getPath.toUri.toString
          // Cache only meaningful partition values (non-empty row)
          if (pvalues.numFields > 0) {
            partValuesByPath.update(pStr, pvalues)
          }
          val base = PartitionedFileUtil.getPartitionedFile(f, f.getPath, pvalues)
          // Cache only non-empty preferred locations
          val locs = base.locations
          if (locs != null && locs.nonEmpty) {
            hostsByPath.update(pStr, locs)
          }
        }
      }
      // Prepare (path, len) pairs
      val pairs = allFiles.map(f => (f.getPath.toUri.toString, f.getLen))
      val slices = FourMcParallelPlanner.expandWithSparkJob(
        sparkSession.sparkContext,
        pairs,
        broadcastConf,
        maxSplitBytes,
        parallelExpandMax
      )
      slices.iterator.map { s =>
        val pv = partValuesByPath.getOrElse(s.path, InternalRow.empty)
        val hosts = hostsByPath.getOrElse(s.path, Array.empty[String])
        PartitionedFile(pv, s.path, s.start, s.length, hosts)
      }.toSeq.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    } else {
      selectedPartitions.flatMap { partition =>
        // Prune partition values if part of the partition columns are not required.
        val partitionValues = if (readPartitionAttributes != partitionAttributes) {
          partitionValueProject(partition.values).copy()
        } else {
          partition.values
        }
        partition.files.flatMap { file =>
          val filePath = file.getPath
          // Build a single PartitionedFile for the whole file (with proper hosts)
          val base = PartitionedFileUtil.getPartitionedFile(file, filePath, partitionValues)
          // Expand it into 4mc block-aligned slices using the helper
          FourMcBlockPlanner
            .expandPartitionedFile(base, maxSplitBytes, broadcastConf)
        }
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    FilePartition.getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  private def normalizeName(name: String): String = if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)

  /**
   * Provide a reader factory that will create readers per input partition.  A
   * slice may consist of multiple adjacent blocks (planned by
   * FourMcBlockPlanner) and this reader factory will produce readers that
   * iterate through all slices in a partition.  The factory is aware of
   * whether the offset column is required by inspecting the read schema.
   */
  override def createReaderFactory(): PartitionReaderFactory =
    new FourMcPartitionReaderFactory(readDataSchema, readDataSchema.exists(_.name == "offset"), broadcastConf)

  /**
   * Return a new FourMcScan with the provided partition and data filters.
   * Spark calls this method to push filters down into the scan.  Although
   * 4mc cannot currently make use of these filters for block pruning, we
   * retain them so that Spark can apply the filters after reading.  The
   * options and schemas remain unchanged.
   */
  override def withFilters(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]
  ): FileScan = {
    new FourMcScan(
      sparkSession,
      fileIndex,
      readDataSchema,
      options,
      readPartitionSchema,
      partitionFilters,
      dataFilters
    )
  }
}

/**
 * Companion object with helper functions to expand partitioned files along
 * 4mc block boundaries.  The logic is extracted here for clarity.
 */
object FourMcBlockPlanner {

  import com.fing.compression.fourmc.FourMcBlockIndex
  import org.apache.spark.sql.execution.datasources.PartitionedFile

  /**
   * Expand a single PartitionedFile into multiple block-aligned slices.
   * The first slice always starts at 0, so the reader does not skip the
   * first line.  The final slice always ends at the file length to avoid
   * losing the last line.  Each slice is limited in size by
   * `maxPartitionBytes` (but always includes at least one block).
   */
  def expandPartitionedFile(
      pf: PartitionedFile,
      maxPartitionBytes: Long,
      broadcastConf: Broadcast[SerializableConfiguration]
  ): Seq[PartitionedFile] = {
    val conf = broadcastConf.value.value
    val path = new Path(pf.filePath)
    val fs: FileSystem = path.getFileSystem(conf)
    val fileLen = fs.getFileStatus(path).getLen
    val index = FourMcBlockIndex.readIndex(fs, path)
    if (index == null || index.isEmpty) {
      return Seq(pf.copy(start = 0L, length = fileLen))
    }
    val blocks = index.getNumberOfBlocks
    val out = mutable.ArrayBuffer[PartitionedFile]()
    var i = 0
    while (i < blocks) {
      val start = if (i == 0) 0L else index.getPosition(i)
      var end = start
      var acc = 0L
      var j = i
      while (j < blocks && (acc < maxPartitionBytes || j == i)) {
        val cur = index.getPosition(j)
        val next = if (j + 1 < blocks) index.getPosition(j + 1) else fileLen
        acc += (next - cur)
        end = next
        j += 1
      }
      out += pf.copy(start = start, length = (end - start))
      i = j
    }
    out
  }
}
