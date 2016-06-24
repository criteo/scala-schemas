package com.criteo.scalaschemas.hive

import com.criteo.scalaschemas.hive.queries.fragments._

import scala.collection.immutable.Stack

/**
 * A schema for Hive.
 *
 * @param database The database name.
 * @param table The table name.
 * @param columns The ordered list of [[HiveColumn]]s.
 * @param partitionColumns The ordered list of [[HivePartitionColumn]]s.
 * @param hdfsLocation The location on HDFS for the table data.
 * @param storageEngine the storage engine to use.
 * @param external Whether this is an external table or not.
 */
case class HiveSchema(database: String,
                      table: String,
                      columns: HiveColumns,
                      hdfsLocation: String,
                      storageEngine: HiveStorageFormat = TextFileHiveStorageFormat,
                      partitionColumns: Option[HivePartitionColumns] = None,
                      tableProperties: Option[HiveTableProperties] = None,
                      external: Boolean = false) {
  private val partitions = partitionColumns.map(cols => cols.partitionColumns.map(_.name)).getOrElse(Stack.empty)
  private val tableColumns = columns.columns.map(_.name)
  private val intersection = partitions.intersect(tableColumns)
  require(columns.columns.nonEmpty, "The table should have at least one column.")
  require(intersection.isEmpty, s"Duplicated column names in partitionColumns: ${intersection.mkString(", ")}.")
}
