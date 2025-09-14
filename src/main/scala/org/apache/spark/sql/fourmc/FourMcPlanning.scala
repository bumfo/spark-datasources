package org.apache.spark.sql.fourmc

import com.fing.mapreduce.FourMcLineRecordReader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.mutable

case class FourMcPlanning(
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

  /** Compute 4mc block‑aligned slices (PartitionedFile) for all selected files. */
  lazy val splitFiles: Seq[PartitionedFile] = {
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
      val expanded = FourMcParallelPlanner.expandWithSparkJob(
        spark.sparkContext,
        pairs,
        broadcastConf,
        maxSplitBytes,
        parallelExpandMax
      )
      val empty = InternalRow(Array.empty)
      expanded.iterator.map { s =>
        val pv = partValuesByPath.getOrElse(s.path, empty)
        val hosts = hostsByPath.getOrElse(s.path, Array.empty[String])
        PartitionedFile(pv, s.path, s.start, s.length, hosts)
      }.toSeq.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    } else {
      selectedPartitions.flatMap { partition =>
        val partitionValues = if (readPartitionAttributes != partitionAttributes) {
          partitionValueProject(partition.values).copy()
        } else {
          partition.values
        }
        partition.files.flatMap { file =>
          val filePath = file.getPath
          val base = PartitionedFileUtil.getPartitionedFile(file, filePath, partitionValues)
          FourMcBlockPlanner
            .expandPartitionedFile(base, maxSplitBytes, broadcastConf)
        }
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    slices
  }

  /** Convert the planned slices into Spark input partitions for the scan. */
  def filePartitions: Array[org.apache.spark.sql.connector.read.InputPartition] = {
    FilePartition.getFilePartitions(spark, splitFiles, maxSplitBytes).toArray
  }

  /** Build a Dataset[String] reading lines from planned slices using 4mc reader. */
  def datasetOfLines(charsetOpt: Option[String]): Dataset[String] = {
    val sc = spark.sparkContext
    val slices = splitFiles
    val confBc = broadcastConf
    val rdd: RDD[String] = sc.parallelize(slices, slices.size.max(1)).mapPartitions { iter =>
      iter.flatMap { pf =>
        val conf = confBc.value.value
        val path = new Path(pf.filePath)
        val split = new FileSplit(path, pf.start, pf.length, Array.empty)
        val ctx = new TaskAttemptContextImpl(conf, new TaskAttemptID())
        val reader = new FourMcLineRecordReader()
        reader.initialize(split, ctx)
        new Iterator[String] {
          private var hasNextKV: Boolean = reader.nextKeyValue()

          override def hasNext: Boolean = hasNextKV

          override def next(): String = {
            val v: Text = reader.getCurrentValue
            val s = v.toString
            hasNextKV = reader.nextKeyValue()
            s
          }
        }.toIterable
      }
    }
    spark.createDataset(rdd)(Encoders.STRING)
  }
}
