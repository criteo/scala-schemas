package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema

/**
 * A [[HiveQuery]] operating on a table.
 *
 * @param schema The schema of the underlying table.
 */
abstract class HiveTableQuery(schema: HiveSchema) extends HiveQuery
