package org.apache.spark.sql.fourmc

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{AtomicType, DataType, StructType, UserDefinedType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.annotation.tailrec

/**
 * A concrete FileTable for reading 4mc-compressed files.  This mirrors Spark's
 * built-in CSVTable implementation by extending [[FileTable]] and wiring a
 * custom scan builder.  It supports optional user-specified schemas and
 * delegates block-aligned split planning to the underlying scan.
 *
 * @param name                logical name of the table (typically derived from paths)
 * @param sparkSession        active Spark session
 * @param options             options without the `path`/`paths` keys
 * @param paths               list of input paths supplied via `path` or `paths`
 * @param userSpecifiedSchema optional user-provided schema
 * @param fallbackFileFormat  v1 fallback (unused for reading but required by FileTable)
 */
final case class FourMcTable(
                              name: String,
                              sparkSession: SparkSession,
                              options: CaseInsensitiveStringMap,
                              paths: Seq[String],
                              userSpecifiedSchema: Option[StructType],
                              fallbackFileFormat: Class[_ <: FileFormat]
                            ) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  /**
   * Build a custom scan for this table.  The returned builder will plan
   * partitions based on the 4mc footer block index and create readers
   * accordingly.
   */
  override def newScanBuilder(options: CaseInsensitiveStringMap): FourMcScanBuilder = {
    // Pass the partitioning-aware file index to the scan builder.  Spark's
    // FileTable exposes the file index as a PartitioningAwareFileIndex, which
    // retains information about partition columns.  We also forward the
    // remaining options to the builder; the builder will compute the read
    // schema based on the `withOffset` flag and fetch the partition schema
    // from the index.
    new FourMcScanBuilder(sparkSession, fileIndex, options)
  }

  /**
   * Schema inference for 4mc files.  Because 4mc encodes plain text lines, we
   * cannot infer a structured schema beyond what the user provides.  If a
   * schema was specified when the table was created, return it; otherwise
   * return None to use the default text schema.
   */
  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    userSpecifiedSchema
  }

  /**
   * Determine whether a data type is supported for writing.  Since this
   * implementation is read-only and processes text, we accept only atomic
   * types (e.g., String, Int, Long) and user-defined types that reduce to
   * atomic types.
   */
  @tailrec
  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true
    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)
    case _ => false
  }

  /**
   * Human-friendly name for this format.  Used in error messages.
   */
  override def formatName: String = "4MC"

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = throw new NotImplementedError("Only support read")
}