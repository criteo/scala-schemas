package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema

/**
 * A [[com.criteo.scalaschemas.hive.queries.HiveQuery HiveQuery]] that creates a table.
 *
 * @param schema The schema of the underlying table.
 */
case class HiveCreateTableQuery(schema: HiveSchema) extends HiveTableQuery(schema) {

  private val external = schema.external
  private val table = schema.table
  private val columns = schema.columns
  private val partitions = schema.partitionColumns
  private val storageEngine = schema.storageEngine
  private val location = schema.hdfsLocation
  private val tblProperties = schema.tableProperties

  /**
   * Generates a hive 0.11 compliant CREATE TABLE statement, based on the subset of supported
   * features:
   * {{{
   * CREATE [EXTERNAL] TABLE IF NOT EXISTS [db_name.]table_name
   * [(col_name data_type [COMMENT col_comment], ...)]
   * [PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)]
   * [
   *  [ROW FORMAT row_format]
   *  [STORED AS file_format]
   *   | STORED BY 'storage.handler.class.name' [WITH SERDEPROPERTIES (...)]
   * ]
   * [LOCATION hdfs_path]
   * [TBLPROPERTIES (property_name=property_value, ...)]
   * }}}
   * See wiki for full specs:
   * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable
   *
   * @return The query.
   */
  override def make: String =
    s"""CREATE ${if (external) "EXTERNAL " else ""}TABLE IF NOT EXISTS $table
        |${columns.generate}
        |${partitions.map(_.generate).getOrElse("")}
        |${storageEngine.generate}
        |LOCATION '$location'
        |${tblProperties.map(_.generate).getOrElse("")};""".stripMargin

}
