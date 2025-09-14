package org.apache.spark.sql.fourmc.json

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.{CreateJacksonParser, JSONOptions, JSONOptionsInRead, JacksonParser}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.fourmc.{FourMcScan, FourMcScanBuilder, FourMcSchemaAwareDataSource, FourMcTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import scala.annotation.tailrec
import scala.collection.JavaConverters._

final class FourMcJsonFileDataSource extends FourMcSchemaAwareDataSource {
  override def shortName(): String = "fourmc.json"

  override protected def createTable(
      tableName: String,
      options: CaseInsensitiveStringMap,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType]) =
    new FourMcJsonTable(
      name = tableName,
      sparkSession = SparkSession.active,
      options = options,
      paths = paths,
      userSpecifiedSchema = userSpecifiedSchema,
      fallbackFileFormat = fallbackFileFormat)
}

class FourMcJsonTable(
    name: String,
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType],
    fallbackFileFormat: Class[_ <: org.apache.spark.sql.execution.datasources.FileFormat]
) extends FourMcTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat) {
  override protected def buildScanBuilder(): FourMcScanBuilder =
    new FourMcJsonScanBuilder(sparkSession, fileIndex, options, schema)

  override def inferSchema(files: Seq[org.apache.hadoop.fs.FileStatus]): Option[StructType] = {
    val parsedOptions = new JSONOptionsInRead(
      options.asScala.toMap,
      sparkSession.sessionState.conf.sessionLocalTimeZone,
      sparkSession.sessionState.conf.columnNameOfCorruptRecord
    )
    org.apache.spark.sql.execution.datasources.json.JsonDataSource(parsedOptions)
      .inferSchema(sparkSession, files, parsedOptions)
  }
}

final class FourMcJsonScanBuilder(
    spark: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    opts: CaseInsensitiveStringMap,
    readSchema: StructType
) extends FourMcScanBuilder(spark, fileIndex, opts) {
  override lazy val build: Scan = {
    val partitionSchema = fileIndex.partitionSchema
    new FourMcJsonScan(
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

final class FourMcJsonScan(
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
    val parsed = new JSONOptions(options.asCaseSensitiveMap().asScala.toMap, SQLConf.get.sessionLocalTimeZone)
    new FourMcJsonPartitionReaderFactory(readDataSchema, parsed, broadcastConf)
  }
}

final class FourMcJsonPartitionReaderFactory(
    dataSchema: StructType,
    parsedOptions: JSONOptions,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val fp = partition.asInstanceOf[FilePartition]
    new FourMcJsonMultiSliceReader(fp.files.toSeq, dataSchema, parsedOptions, broadcastConf)
  }
}

final class FourMcJsonMultiSliceReader(
    slices: Seq[PartitionedFile],
    dataSchema: StructType,
    parsedOptions: JSONOptions,
    broadcastConf: Broadcast[SerializableConfiguration]
) extends PartitionReader[InternalRow] {
  private var idx = 0
  private var current: FourMcJsonSliceReader = _

  @tailrec
  override def next(): Boolean = {
    if (current == null) {
      if (idx >= slices.length) return false
      current = new FourMcJsonSliceReader(slices(idx), dataSchema, parsedOptions, broadcastConf.value.value)
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

final class FourMcJsonSliceReader(
    pf: PartitionedFile,
    dataSchema: StructType,
    jsonOpts: JSONOptions,
    conf: Configuration
) extends PartitionReader[InternalRow] {
  private val delegate = new org.apache.spark.sql.fourmc.FourMcSliceReader(
    pf,
    StructType(Seq(org.apache.spark.sql.types.StructField("value", org.apache.spark.sql.types.StringType, nullable = true))),
    false,
    conf
  )
  private val parser = new JacksonParser(dataSchema, jsonOpts, allowArrayAsStructs = false)
  private var current: InternalRow = _

  override def next(): Boolean = {
    while (delegate.next()) {
      val v: UTF8String = delegate.get().getUTF8String(0)
      val rows = parser.parse[UTF8String](v, CreateJacksonParser.utf8String, identity)
      val it = rows.iterator
      if (it.hasNext) {
        current = it.next(); return true
      }
    }
    false
  }

  override def get(): InternalRow = current

  override def close(): Unit = delegate.close()
}
