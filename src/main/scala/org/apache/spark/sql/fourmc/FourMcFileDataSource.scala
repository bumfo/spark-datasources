package org.apache.spark.sql.fourmc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Entry point for the 4mc reader built on Spark's native file source API (V2).
 *
 * This class extends [[org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2]]
 * so that Spark can leverage its built-in distributed file discovery and
 * partition pruning logic. It also implements [[DataSourceRegister]] to
 * provide the short name "fourmc".  Unlike earlier versions, this data
 * source directly constructs a [[FourMcTable]] rather than delegating
 * through a legacy V1 provider.  It extracts path options and strips them
 * from the remaining options, mirroring Spark's built-in CSV and JSON data
 * sources.
 */
class FourMcFileDataSource
  extends FileDataSourceV2
    with DataSourceRegister {

  /**
   * The short name of this data source.  Users can simply use
   * `spark.read.format("fourmc")` to access it.
   */
  override def shortName(): String = "fourmc"

  /**
   * Returns a table with inferred schema.  Spark calls this method when
   * schema inference is required.  We build a [[FourMcTable]] which will
   * infer a schema consisting of an optional offset column and a value
   * column.  Options specifying `withOffset=true` influence the schema.
   */
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = parsePaths(options)
    require(paths.nonEmpty, "Option 'path' or 'paths' must be specified for fourmc datasource")
    val cleaned = dropPathOptions(options)
    val tableName = computeTableName(paths)
    FourMcTable(
      name = tableName,
      sparkSession = SparkSession.active,
      options = cleaned,
      paths = paths,
      userSpecifiedSchema = None,
      fallbackFileFormat = fallbackFileFormat
    )
  }

  /**
   * The legacy file format associated with this data source.  We use
   * Spark's built‑in text format as the fallback since 4mc stores text
   * lines compressed with LZ4.  This fallback is rarely used because
   * this implementation provides its own scan.
   */
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[TextFileFormat]

  /**
   * Parse the list of input paths from the provided options.  Spark's
   * built-in file sources support a single `path` or a comma-separated
   * list via the `paths` option.  We implement similar logic here.
   */
  protected def parsePaths(options: CaseInsensitiveStringMap): Seq[String] = {
    val paths = new scala.collection.mutable.ArrayBuffer[String]()
    val pathOpt = Option(options.get("path")).map(_.trim).filter(_.nonEmpty)
    pathOpt.foreach(paths += _)
    val multi = Option(options.get("paths")).map(_.split(",")).getOrElse(Array.empty)
    multi.foreach { p =>
      val trimmed = p.trim
      if (trimmed.nonEmpty) paths += trimmed
    }
    paths.distinct
  }

  /**
   * Remove the `path` and `paths` keys from the options map.  Spark passes
   * the full options to the underlying table; however, the file source
   * machinery treats `path` and `paths` specially.  We therefore strip
   * these keys so that the remaining options can be consumed by the
   * table and scan implementations.
   */
  protected def dropPathOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val map = new java.util.HashMap[String, String]()
    val iter = options.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val k = entry.getKey
      if (!k.equalsIgnoreCase("path") && !k.equalsIgnoreCase("paths")) {
        map.put(k, entry.getValue)
      }
    }
    new CaseInsensitiveStringMap(map)
  }

  /**
   * Construct a table name from the provided paths.  Spark's built-in file
   * sources use the first path as the table name when multiple paths are
   * provided.  We follow the same convention.
   */
  protected def computeTableName(paths: Seq[String]): String = {
    if (paths.isEmpty) "4mc"
    else {
      // Use the last component of the first path as the table name.
      val first = paths.head
      val lastSlash = first.lastIndexOf('/')
      if (lastSlash >= 0 && lastSlash < first.length - 1) first.substring(lastSlash + 1)
      else first
    }
  }
}
