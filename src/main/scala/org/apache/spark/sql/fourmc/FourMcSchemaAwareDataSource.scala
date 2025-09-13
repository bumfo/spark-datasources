package org.apache.spark.sql.fourmc

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Helper base to DRY the (options) and (options, schema) getTable overloads.
 * Subclasses provide a concrete table via `createTable`.
 */
abstract class FourMcSchemaAwareDataSource extends FourMcFileDataSource {

  protected def createTable(
      tableName: String,
      options: CaseInsensitiveStringMap,
      paths: Seq[String],
      userSpecifiedSchema: Option[StructType]): Table

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = parsePaths(options)
    require(paths.nonEmpty, "Option 'path' or 'paths' must be specified")
    val cleaned = dropPathOptions(options)
    val tableName = computeTableName(paths)
    createTable(tableName, cleaned, paths, None)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = parsePaths(options)
    require(paths.nonEmpty, "Option 'path' or 'paths' must be specified")
    val cleaned = dropPathOptions(options)
    val tableName = computeTableName(paths)
    createTable(tableName, cleaned, paths, Some(schema))
  }
}

