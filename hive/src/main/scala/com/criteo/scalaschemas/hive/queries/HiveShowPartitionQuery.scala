package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.queries.fragments.HivePartitionValues

/**
 * A [[com.criteo.scalaschemas.hive.queries.HiveQuery HiveQuery]]
 * that show partition(s) of a table.
 *
 * @param tableName The name of the table.
 * @param partition The partition to restrict the show partition to, if any.
 */
case class HiveShowPartitionQuery(tableName: String,
                                  partition: Option[HivePartitionValues]) extends HiveQuery {
  require(partition.map(_.allValuesDefined).getOrElse(true), "All values of the partition should be defined")

  /**
   * Makes a query in the form:
   * {{{
   * SHOW PARTITIONS $table [PARTITION (...)]
   * }}}
   * @return The query.
   */
  override def make: String = s"SHOW PARTITIONS $tableName${partition.map(" " + _.generate).getOrElse("")};"
}
