package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments.HivePartitionValues

/**
 * A [[com.criteo.scalaschemas.hive.queries.HiveQuery HiveQuery]] that inserts data into a table.
 * Used for ETL type work, typically.
 *
 * @param schema The schema of the underlying table.
 * @param outputQuery The insertion query.
 * @param partition The partition to insert into, if any.
 */
case class HiveOutputQuery(schema: HiveSchema,
                           outputQuery: String,
                           partition: Option[HivePartitionValues]) extends HiveTableQuery(schema) {

  /**
   * Makes a query in the form:
   * {{{
   * INSERT OVERWRITE TABLE tableName
   * [PARTITION (`partitionKey1`='partitionValue1', ...)]
   * SELECT ...
   * }}}
   * @return The query.
   */
  override def make: String =
    s"""INSERT OVERWRITE TABLE ${schema.table}
        |${partition.map(_.generate).getOrElse("")}
        |$outputQuery;""".stripMargin

}
