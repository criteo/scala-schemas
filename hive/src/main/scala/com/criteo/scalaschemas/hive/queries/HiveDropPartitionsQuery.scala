package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments.HivePartitionValues

/**
 * A [[com.criteo.scalaschemas.hive.queries.HiveQuery HiveQuery]]
 * that drops one or more partitions from a table.
 *
 * @param schema The schema of the underlying table.
 * @param partitions The partitions to drop.
 */
case class HiveDropPartitionsQuery(schema: HiveSchema,
                                   partitions: Traversable[HivePartitionValues]) extends HiveTableQuery(schema) {
  require(partitions.forall(_.allValuesDefined), "All values of each partition should be defined")

  /**
   * Makes a query in the form:
   * {{{
   * ALTER TABLE $schema.table DROP PARTITION (...);
   * [ALTER TABLE $schema.table DROP PARTITION (...);
   * ...]
   * }}}
   * @return The query.
   */
  override def make: String =
    partitions.map { partition =>
      s"ALTER TABLE ${schema.table} DROP ${partition.generate};"
    }.mkString("\n")
}
