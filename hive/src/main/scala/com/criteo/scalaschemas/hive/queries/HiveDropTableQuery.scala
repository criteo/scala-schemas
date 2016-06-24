package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema

/**
 * A [[com.criteo.scalaschemas.hive.queries.HiveQuery HiveQuery]] that drops a table.
 *
 * @param schema The schema of the underlying table.
 */
case class HiveDropTableQuery(schema: HiveSchema) extends HiveTableQuery(schema) {

  override def make: String =
    s"""DROP TABLE IF EXISTS ${schema.table};"""

}
