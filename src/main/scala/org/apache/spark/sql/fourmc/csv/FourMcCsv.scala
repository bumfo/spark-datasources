package org.apache.spark.sql.fourmc.csv

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.types.{StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import org.apache.spark.sql.fourmc.{FourMcFileDataSource, FourMcScan, FourMcScanBuilder, FourMcSliceReader, FourMcTable}

import scala.collection.JavaConverters._
import org.apache.spark.sql.catalyst.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.internal.SQLConf

/** DataSource short name fourmc.csv */
final class FourMcCsvFileDataSource extends FourMcFileDataSource {
  override def shortName(): String = "fourmc.csv"
  override def getTable(options: CaseInsensitiveStringMap) = {
    val paths = parsePaths(options)
    require(paths.nonEmpty, "Option 'path' or 'paths' must be specified for fourmc.csv datasource")
    val cleaned = dropPathOptions(options)
    val tableName = computeTableName(paths)
    new FourMcCsvTable(
      name = tableName,
      sparkSession = SparkSession.active,
      options = cleaned,
      paths = paths,
      userSpecifiedSchema = None,
      fallbackFileFormat = fallbackFileFormat
    )
  }
}

class FourMcCsvTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: org.apache.spark.sql.execution.datasources.FileFormat]
) extends FourMcTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {
  override protected def buildScanBuilder(): FourMcScanBuilder =
    new FourMcCsvScanBuilder(sparkSession, fileIndex, options)
}

final class FourMcCsvScanBuilder(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    opts: CaseInsensitiveStringMap
) extends FourMcScanBuilder(spark, fileIndex, opts) {
  override lazy val build: Scan = {
    val withOffset: Boolean =
      java.lang.Boolean.parseBoolean(options.getOrDefault("withOffset", "false"))
    val readSchema = if (withOffset) {
      StructType(Seq(
        org.apache.spark.sql.types.StructField("offset", org.apache.spark.sql.types.LongType, false),
        org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, true)
      ))
    } else {
      StructType(Seq(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, true)))
    }
    val partitionSchema = fileIndex.partitionSchema
    new FourMcCsvScan(
      spark,
      fileIndex,
      readSchema,
      options,
      partitionSchema,
      Seq.empty,
      Seq.empty
    )
  }
}

final class FourMcCsvScan(
    override val sparkSession: SparkSession,
    override val fileIndex: PartitioningAwareFileIndex,
    override val readDataSchema: StructType,
    options: CaseInsensitiveStringMap,
    override val readPartitionSchema: StructType,
    override val partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
    override val dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression]
) extends FourMcScan(sparkSession, fileIndex, readDataSchema, options, readPartitionSchema, partitionFilters, dataFilters) {
  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastConf: Broadcast[SerializableConfiguration] =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(sparkSession.sessionState.newHadoopConf()))
    new FourMcCsvPartitionReaderFactory(readDataSchema, options, broadcastConf)
  }
}

final class FourMcCsvPartitionReaderFactory(
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val fp = partition.asInstanceOf[FilePartition]
    new FourMcCsvMultiSliceReader(fp.files.toSeq, dataSchema, options, broadcastConf)
  }
}

final class FourMcCsvMultiSliceReader(
    slices: Seq[PartitionedFile],
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReader[InternalRow] {
  private var idx = 0
  private var current: FourMcCsvSliceReader = _
  override def next(): Boolean = {
    if (current == null) {
      if (idx >= slices.length) return false
      current = new FourMcCsvSliceReader(slices(idx), dataSchema, options, broadcastConf.value.value)
      idx += 1
    }
    if (current.next()) true else { current.close(); current = null; next() }
  }
  override def get(): InternalRow = current.get()
  override def close(): Unit = if (current != null) current.close()
}

final class FourMcCsvSliceReader(
    pf: PartitionedFile,
    dataSchema: StructType,
    options: CaseInsensitiveStringMap,
    conf: Configuration
) extends PartitionReader[InternalRow] {
  private val delegate = new FourMcSliceReader(
    pf,
    StructType(Seq(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, true))),
    false,
    conf
  )
  private val csvOpts = new CSVOptions(options.asCaseSensitiveMap().asScala.toMap, columnPruning = true, SQLConf.get.sessionLocalTimeZone)
  private val parser = new UnivocityParser(dataSchema, csvOpts)
  private var current: InternalRow = _

  override def next(): Boolean = {
    while (delegate.next()) {
      val v = delegate.get().getUTF8String(0).toString
      val parsed = parser.parse(v)
      if (parsed.isDefined) { current = parsed.get; return true }
    }
    false
  }
  override def get(): InternalRow = current
  override def close(): Unit = delegate.close()
}
