package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments.HivePartitionDefinition

/**
 * A [[com.criteo.scala.langoustine.job.task.hive.queries.HiveQuery HiveQuery]]
 * that adds one or more partitions to a table.
 *
 * @param schema The schema of the underlying table.
 * @param partitionDefinitions The partitions to add.
 */
case class HiveAddPartitionsQuery(schema: HiveSchema,
                                  partitionDefinitions: Traversable[HivePartitionDefinition])
  extends HiveTableQuery(schema) {
  require(partitionDefinitions.forall(_.values.allValuesDefined), "All values of each partition should be defined")

  /**
   * Makes a query in the form:
   * {{{
   * ALTER TABLE tableName ADD PARTITION (...)[ LOCATION '...'];
   * [ALTER TABLE tableName ADD PARTITION...
   * ...]
   * }}}
   * @return The query
   */
  override def make: String = partitionDefinitions.map { partitionDef =>
    s"ALTER TABLE ${schema.table} ADD ${partitionDef.generate};"
  }.mkString("\n")
}
