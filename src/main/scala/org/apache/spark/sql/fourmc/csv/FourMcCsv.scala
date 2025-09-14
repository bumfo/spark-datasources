package org.apache.spark.sql.fourmc.csv

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVOptions, UnivocityParser}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.fourmc.{FourMcPlanning, FourMcScan, FourMcScanBuilder, FourMcSchemaAwareDataSource, FourMcSliceReader, FourMcTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.annotation.tailrec
import scala.collection.JavaConverters._

final class FourMcCSVFileDataSource extends FourMcSchemaAwareDataSource {
  override def shortName(): String = "fourmc.csv"

  override protected def createTable(
      tableName: String,
      options: CaseInsensitiveStringMap,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType]) =
    new FourMcCSVTable(
      name = tableName,
      sparkSession = SparkSession.active,
      options = options,
      paths = paths,
      userSpecifiedSchema = userSpecifiedSchema,
      fallbackFileFormat = fallbackFileFormat)
}

class FourMcCSVTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: org.apache.spark.sql.execution.datasources.FileFormat]
) extends FourMcTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {
  override protected def buildScanBuilder(): FourMcScanBuilder =
    new FourMcCSVScanBuilder(sparkSession, fileIndex, options, schema, planning)

  override def inferSchema(files: Seq[org.apache.hadoop.fs.FileStatus]): Option[StructType] = {
    val parsed = new CSVOptions(
      options.asScala.toMap,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      sparkSession.sessionState.conf.sessionLocalTimeZone
    )
    val ds = planning.datasetOfLines(Some(parsed.charset))
    val first = org.apache.spark.sql.execution.datasources.csv.CSVUtils.filterCommentAndEmpty(ds, parsed).take(1).headOption
    val struct = org.apache.spark.sql.execution.datasources.csv.TextInputCSVDataSource
      .inferFromDataset(sparkSession, ds, first, parsed)
    Some(struct)
  }
}

final class FourMcCSVScanBuilder(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    opts: CaseInsensitiveStringMap,
    readSchema: StructType,
    planning: FourMcPlanning
) extends FourMcScanBuilder(spark, fileIndex, opts, planning) {
  override lazy val build: Scan = {
    val partitionSchema = fileIndex.partitionSchema
    new FourMcCSVScan(
      spark,
      fileIndex,
      readSchema,
      options,
      partitionSchema,
      Seq.empty,
      Seq.empty,
      planning
    )
  }
}

final class FourMcCSVScan(
    override val sparkSession: SparkSession,
    override val fileIndex: PartitioningAwareFileIndex,
    override val readDataSchema: StructType,
    options: CaseInsensitiveStringMap,
    override val readPartitionSchema: StructType,
    override val partitionFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
    override val dataFilters: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
    planning: FourMcPlanning
) extends FourMcScan(sparkSession, fileIndex, readDataSchema, options, readPartitionSchema, partitionFilters, dataFilters, planning) {
  override def createReaderFactory(): PartitionReaderFactory = {
    val broadcastConf: Broadcast[SerializableConfiguration] =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(sparkSession.sessionState.newHadoopConf()))
    val parsed = new CSVOptions(
      options.asCaseSensitiveMap().asScala.toMap,
      columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
      SQLConf.get.sessionLocalTimeZone
    )
    new FourMcCSVPartitionReaderFactory(readDataSchema, parsed, broadcastConf)
  }
}

final class FourMcCSVPartitionReaderFactory(
    dataSchema: StructType,
    parsedOptions: CSVOptions,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val fp = partition.asInstanceOf[FilePartition]
    new FourMcCSVMultiSliceReader(fp.files.toSeq, dataSchema, parsedOptions, broadcastConf)
  }
}

final class FourMcCSVMultiSliceReader(
    slices: Seq[PartitionedFile],
    dataSchema: StructType,
    parsedOptions: CSVOptions,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReader[InternalRow] {
  private var idx = 0
  private var current: FourMcCSVSliceReader = _

  @tailrec
  override def next(): Boolean = {
    if (current == null) {
      if (idx >= slices.length) return false
      current = new FourMcCSVSliceReader(slices(idx), dataSchema, parsedOptions, broadcastConf.value.value)
      idx += 1
    }
    if (current.next()) true else {
      current.close();
      current = null;
      next()
    }
  }

  override def get(): InternalRow = current.get()

  override def close(): Unit = if (current != null) current.close()
}

final class FourMcCSVSliceReader(
    pf: PartitionedFile,
    dataSchema: StructType,
    csvOpts: CSVOptions,
    conf: Configuration
) extends PartitionReader[InternalRow] {
  private val delegate = new FourMcSliceReader(
    pf,
    StructType(Seq(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, nullable = true))),
    false,
    conf
  )
  private val parser = new UnivocityParser(dataSchema, csvOpts)
  private var current: InternalRow = _
  private var headerSkipped = false

  override def next(): Boolean = {
    // Drop header on the very first slice start of a file if requested
    if (!headerSkipped && csvOpts.headerFlag && pf.start == 0L) {
      if (delegate.next()) {
        /* discard one header record */
      }
      headerSkipped = true
    }
    while (delegate.next()) {
      val v = delegate.get().getUTF8String(0).toString
      val parsed = parser.parse(v)
      if (parsed.isDefined) {
        current = parsed.get;
        return true
      }
    }
    false
  }

  override def get(): InternalRow = current

  override def close(): Unit = delegate.close()
}
