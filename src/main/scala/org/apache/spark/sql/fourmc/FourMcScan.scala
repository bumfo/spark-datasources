package org.apache.spark.sql.fourmc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
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
class FourMcScanBuilder(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    val options: CaseInsensitiveStringMap,
    planner: FourMcPlanner
) extends ScanBuilder {

  // Determine whether to include the offset column.  We re-compute the data
  // schema accordingly when building the scan.
  private val withOffset: Boolean =
    java.lang.Boolean.parseBoolean(options.getOrDefault("withOffset", "false"))

  import org.apache.spark.sql.types.{LongType, StringType, StructField}

  override lazy val build: Scan = {
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
      dataFilters = Seq.empty,
      planner = planner
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
class FourMcScan(
    override val sparkSession: SparkSession,
    override val fileIndex: PartitioningAwareFileIndex,
    override val readDataSchema: StructType,
    options: CaseInsensitiveStringMap,
    override val readPartitionSchema: StructType,
    override val partitionFilters: Seq[Expression],
    override val dataFilters: Seq[Expression],
    planner: FourMcPlanner
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
  override lazy val planInputPartitions: Array[InputPartition] = planner.filePartitions

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
      dataFilters,
      planner.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)
    )
  }
}

/**
 * Companion object with helper functions to expand partitioned files along
 * 4mc block boundaries.  The logic is extracted here for clarity.
 */
// Block and parallel planners moved under FourMcPlanning companion.
