package com.example.fourmc.datasource

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptID}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.JavaConverters._

import com.fing.compression.fourmc.FourMcBlockIndex
import com.fing.mapreduce.FourMcLineRecordReader
import com.fing.compression.fourmc.util.HadoopUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.util.SerializableConfiguration

/**
 * DataSource V2 for reading 4mc-compressed text files into Spark DataFrames.  This
 * implementation honors the 4mc footer block index when planning splits, avoiding
 * mid-block starts and ensuring parallelism.  It does not rely on the legacy
 * newAPIHadoopFile path.
 */
class FourMcDataSource extends TableProvider with DataSourceRegister with Logging {
  /**
   * Short name used to identify this data source.  Enables `.format("fourmc")`.
   */
  override def shortName(): String = "fourmc"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Two columns: optional offset and value string
    val withOffset = java.lang.Boolean.parseBoolean(options.getOrDefault("withOffset", "false"))
    if (withOffset) {
      StructType(Seq(
        StructField("offset", LongType, nullable = false),
        StructField("value", StringType, nullable = true)
      ))
    } else {
      StructType(Seq(
        StructField("value", StringType, nullable = true)
      ))
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String],
      options: CaseInsensitiveStringMap
  ): Table = {
    new FourMcTable(schema, options)
  }

  override def supportsExternalMetadata(): Boolean = true
}

private class FourMcTable(schema: StructType, options: CaseInsensitiveStringMap)
    extends Table with SupportsRead with Logging {

  override def name(): String = "fourmc"

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new FourMcScanBuilder(schema, options)
  }
}

private class FourMcScanBuilder(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends ScanBuilder with Logging {
  override def build(): Scan = new FourMcScan(schema, options)
}

private class FourMcScan(
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends Scan with Batch with Logging {

  private val withOffset: Boolean = java.lang.Boolean.parseBoolean(options.getOrDefault("withOffset", "false"))
  private val encoding: String = Option(options.get("encoding")).getOrElse("UTF-8")
  private val maxPartitionBytes: Long = Option(options.get("maxPartitionBytes")).map(_.toLong).getOrElse {
    // default to Spark's files.maxPartitionBytes if available
    val spark = SparkSession.active
    try {
      spark.sessionState.conf.filesMaxPartitionBytes
    } catch {
      case _: Throwable => 128L * 1024L * 1024L // 128 MiB fallback
    }
  }

  // Parse input paths from options (path or paths)
  private val paths: Seq[String] = {
    val single = Option(options.get("path")).toSeq
    val multi  = Option(options.get("paths")).map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq).getOrElse(Seq.empty)
    val all    = (single ++ multi).distinct
    require(all.nonEmpty, "Option 'path' or 'paths' must be specified for fourmc datasource")
    all
  }

  // Use a broadcasted Hadoop Configuration for tasks.  Leveraging Spark's built-in
  // SerializableConfiguration avoids defining our own wrapper class.
  private val spark: SparkSession = SparkSession.active
  private val hadoopConf: Configuration = spark.sessionState.newHadoopConf()
  private val confBroadcast: Broadcast[SerializableConfiguration] = {
    // broadcast the Hadoop conf via SparkContext
    spark.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def description(): String = "FourMcScan"

  override def planInputPartitions(): Array[InputPartition] = {
    val conf = hadoopConf
    val maxBytes = maxPartitionBytes
    val partList = new scala.collection.mutable.ArrayBuffer[InputPartition]
    // iterate over each input path
    for (p <- paths) {
      val path = new Path(p)
      val fs = path.getFileSystem(conf)
      val status = fs.getFileStatus(path)
      if (status.isDirectory) {
        val files = fs.listStatus(path).filter(!_.isDirectory).filter { st =>
          val name = st.getPath.getName
          !name.startsWith(".") && !name.startsWith("_") && com.fing.compression.fourmc.FourMcInputFormatUtil.is4mcFile(name)
        }
        files.foreach { fileStatus =>
          planFile(fileStatus.getPath, maxBytes, partList)
        }
      } else {
        // single file
        planFile(path, maxBytes, partList)
      }
    }
    partList.toArray
  }

  private def planFile(file: Path, maxBytes: Long, partList: scala.collection.mutable.ArrayBuffer[InputPartition]): Unit = {
    val conf = hadoopConf
    val fs = file.getFileSystem(conf)
    val fileSize = fs.getFileStatus(file).getLen
    // read block index
    val index = FourMcBlockIndex.readIndex(fs, file)
    if (index == null || index.isEmpty) {
      // no index, treat as one partition
      partList += FourMcInputPartition(file.toString, 0L, fileSize, withOffset, encoding, confBroadcast)
      return
    }
    // compute block offsets
    val blockCount = index.getNumberOfBlocks
    // accumulate splits
    var blockStartIdx = 0
    var accumulatedBytes: Long = 0L
    while (blockStartIdx < blockCount) {
      // Use 0L as the start of the first slice to avoid skipping the first line.
      val startPos = if (blockStartIdx == 0) 0L else index.getPosition(blockStartIdx)
      var endPos: Long = startPos
      var i = blockStartIdx
      accumulatedBytes = 0L
      // accumulate blocks until threshold, but ensure at least one block per partition
      while (i < blockCount && (accumulatedBytes < maxBytes || i == blockStartIdx)) {
        val currentOffset = index.getPosition(i)
        val nextOffset = if (i + 1 < blockCount) index.getPosition(i + 1) else fileSize
        val blockSize = nextOffset - currentOffset
        accumulatedBytes += blockSize
        endPos = nextOffset
        i += 1
      }
      partList += FourMcInputPartition(file.toString, startPos, endPos, withOffset, encoding, confBroadcast)
      blockStartIdx = i
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new FourMcPartitionReaderFactory(schema, withOffset, encoding, confBroadcast)
  }
}

// InputPartition representing a single 4mc block-aligned slice
private case class FourMcInputPartition(
    filePath: String,
    start: Long,
    end: Long,
    withOffset: Boolean,
    encoding: String,
    confBroadcast: Broadcast[SerializableConfiguration]
) extends InputPartition

private class FourMcPartitionReaderFactory(
    schema: StructType,
    withOffset: Boolean,
    encoding: String,
    confBroadcast: Broadcast[SerializableConfiguration]
) extends PartitionReaderFactory with Logging {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val part = partition.asInstanceOf[FourMcInputPartition]
    new FourMcPartitionReader(part, schema, withOffset, encoding)
  }
}

private class FourMcPartitionReader(
    partition: FourMcInputPartition,
    schema: StructType,
    withOffset: Boolean,
    encoding: String
) extends PartitionReader[InternalRow] with Logging {

  private val fsConf: Configuration = partition.confBroadcast.value.value
  private val filePath = new Path(partition.filePath)
  private var reader: FourMcLineRecordReader = _
  private var context: TaskAttemptContextImpl = _
  private var finished: Boolean = false

  // reused row
  private val row: GenericInternalRow = new GenericInternalRow(schema.length)

  init()

  private def init(): Unit = {
    val conf = new Configuration(fsConf)
    // ensure 4mc codec is on classpath by specifying the codec classes in conf
    // but typically the environment will have it registered
    val split = new FileSplit(filePath, partition.start, partition.end - partition.start, Array.empty)
    val taid = new TaskAttemptID()
    context = new TaskAttemptContextImpl(conf, taid)
    reader = new FourMcLineRecordReader()
    reader.initialize(split, context)
  }

  override def next(): Boolean = {
    if (finished) return false
    val hasNext = reader.nextKeyValue()
    if (!hasNext) {
      finished = true
      false
    } else {
      true
    }
  }

  override def get(): InternalRow = {
    val key: Long = reader.getCurrentKey.get
    val valueText: Text = reader.getCurrentValue
    if (withOffset) {
      row.update(0, key)
      row.update(1, org.apache.spark.unsafe.types.UTF8String.fromString(valueText.toString))
    } else {
      row.update(0, org.apache.spark.unsafe.types.UTF8String.fromString(valueText.toString))
    }
    row
  }

  override def close(): Unit = {
    try {
      if (reader != null) reader.close()
    } catch {
      case e: Throwable => logWarning("Error closing FourMcLineRecordReader", e)
    }
  }
}

// Using Spark's org.apache.spark.util.SerializableConfiguration instead of a
// custom implementation.  No need to define our own wrapper class here.
