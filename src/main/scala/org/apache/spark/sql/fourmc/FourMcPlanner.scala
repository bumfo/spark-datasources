package org.apache.spark.sql.fourmc

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.mutable

case class FourMcPlanner(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    options: CaseInsensitiveStringMap,
    readPartitionSchema: StructType,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty,
    broadcastConf: Broadcast[SerializableConfiguration]
) {

  private val isCaseSensitive: Boolean =
    Option(options.get("caseSensitive")).map(_.toBoolean)
      .orElse(Option(options.get("spark.sql.caseSensitive")).map(_.toBoolean))
      .getOrElse(spark.sessionState.conf.caseSensitiveAnalysis)

  private val parallelExpandEnabled: Boolean =
    java.lang.Boolean.parseBoolean(options.getOrDefault("fourmc.parallelExpand.enabled", "true"))
  private val parallelExpandThreshold: Int =
    Option(options.get("fourmc.parallelExpand.threshold")).map(_.toInt)
      .getOrElse(spark.sessionState.conf.parallelPartitionDiscoveryThreshold)
  private val parallelExpandMax: Int =
    Option(options.get("fourmc.parallelExpand.maxParallelism")).map(_.toInt)
      .getOrElse(spark.sessionState.conf.parallelPartitionDiscoveryParallelism)

  private val maxSplitBytes: Long = FilePartition.maxSplitBytes(spark, fileIndex.listFiles(Seq.empty, Seq.empty))

  private def normalizeName(name: String): String = if (isCaseSensitive) name else name.toLowerCase(Locale.ROOT)

  /** Compute FilePartitions by expanding 4mc slices and coalescing by target size. */
  lazy val filePartitions: Array[InputPartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)

    val partitionAttributes: Seq[Attribute] = fileIndex.partitionSchema.toAttributes
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

    val slices: Seq[PartitionedFile] = if (useParallel) {
      val partValuesByPath = mutable.HashMap.empty[String, org.apache.spark.sql.catalyst.InternalRow]
      val hostsByPath = mutable.HashMap.empty[String, Array[String]]
      selectedPartitions.foreach { partition =>
        val pvalues = if (readPartitionAttributes != partitionAttributes) {
          partitionValueProject(partition.values).copy()
        } else partition.values
        partition.files.foreach { f =>
          val pStr = f.getPath.toUri.toString
          if (pvalues.numFields > 0) partValuesByPath.update(pStr, pvalues)
          val base = PartitionedFileUtil.getPartitionedFile(f, f.getPath, pvalues)
          val locs = base.locations
          if (locs != null && locs.nonEmpty) hostsByPath.update(pStr, locs)
        }
      }
      val pairs = allFiles.map(f => (f.getPath.toUri.toString, f.getLen))
      val expanded = FourMcPlanner.expandWithSparkJob(
        spark.sparkContext,
        pairs,
        broadcastConf,
        maxSplitBytes,
        parallelExpandMax
      )
      val empty = InternalRow(Array.empty)
      expanded.map { s =>
        val pv = partValuesByPath.getOrElse(s.path, empty)
        val hosts = hostsByPath.getOrElse(s.path, Array.empty[String])
        PartitionedFile(pv, s.path, s.start, s.length, hosts)
      }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    } else {
      selectedPartitions.iterator.flatMap { partition =>
        val partitionValues = if (readPartitionAttributes != partitionAttributes) {
          partitionValueProject(partition.values).copy()
        } else {
          partition.values
        }
        partition.files.iterator.flatMap { file =>
          val filePath = file.getPath
          val base = PartitionedFileUtil.getPartitionedFile(file, filePath, partitionValues)
          FourMcPlanner.expandPartitionedFile(base, maxSplitBytes, broadcastConf)
        }
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    FilePartition.getFilePartitions(spark, slices, maxSplitBytes).toArray
  }

  /** Build a Dataset[String] reading lines from planned partitions using FileScanRDD. */
  def datasetOfLines(charsetOpt: Option[String]): Dataset[String] = {
    val parts = filePartitions.map(_.asInstanceOf[FilePartition]).toSeq
    val dataSchema = StructType(Seq(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, nullable = true)))
    val readFunction: PartitionedFile => Iterator[InternalRow] = { pf =>
      val reader = new FourMcSliceReader(pf, dataSchema, withOffset = false, broadcastConf.value.value)
      FourMcPlanner.iteratorFromReader(reader)
    }
    val rdd: RDD[InternalRow] = new FileScanRDD(
      spark,
      readFunction,
      parts
    )
    val df = spark.internalCreateDataFrame(rdd, dataSchema)
    df.select("value").as[String](Encoders.STRING)
  }
}

object FourMcPlanner {

  import com.fing.compression.fourmc.FourMcBlockIndex
  import org.apache.spark.sql.execution.datasources.PartitionedFile

  /** Expand a single PartitionedFile into block-aligned slices using the 4mc footer index. */
  def expandPartitionedFile(
      pf: PartitionedFile,
      maxPartitionBytes: Long,
      broadcastConf: Broadcast[SerializableConfiguration]
  ): Seq[PartitionedFile] = {
    val conf = broadcastConf.value.value
    val path = new Path(pf.filePath)
    val fs: FileSystem = path.getFileSystem(conf)
    val fileLen = pf.length
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

  /** Parallel expansion helper using Spark executors to compute slices for many files. */
  final case class Slice(path: String, start: Long, length: Long) extends Serializable

  def expandWithSparkJob(
      sc: org.apache.spark.SparkContext,
      files: Seq[(String, Long)],
      confBroadcast: Broadcast[SerializableConfiguration],
      maxSplitBytes: Long,
      parallelismMax: Int
  ): Seq[Slice] = {
    if (files.isEmpty) return Seq.empty[Slice]
    val numTasks = math.min(files.size, math.max(1, parallelismMax))
    val rdd = sc.parallelize(files, numTasks)

    val previous = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
    val desc = files.size match {
      case 0 => "FourMc: expand slices for 0 files"
      case 1 => s"FourMc: expand slices for 1 file:<br/>${files.head._1}"
      case n => s"FourMc: expand slices for $n files:<br/>${files.head._1}, ..."
    }

    val slices = try {
      sc.setJobDescription(desc)
      rdd.mapPartitions { iter =>
        iter.flatMap { case (path, len) =>
          val base = PartitionedFile(InternalRow.empty, path, 0L, len, Array.empty[String])
          FourMcPlanner
            .expandPartitionedFile(base, maxSplitBytes, confBroadcast)
            .map(pf => Slice(pf.filePath, pf.start, pf.length))
        }
      }.collect()
    } finally {
      sc.setJobDescription(previous)
    }

    slices.toSeq
  }

  /** Convert a PartitionReader into an Iterator that auto-closes on exhaustion. */

  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.connector.read.PartitionReader

  def iteratorFromReader(reader: PartitionReader[InternalRow]): Iterator[InternalRow] = new Iterator[InternalRow] {
    private var open = true
    private var hasNextRow = reader.next()

    override def hasNext: Boolean = open && hasNextRow

    override def next(): InternalRow = {
      val row = reader.get()
      hasNextRow = reader.next()
      if (!hasNextRow) {
        reader.close();
        open = false
      }
      row
    }
  }
}
