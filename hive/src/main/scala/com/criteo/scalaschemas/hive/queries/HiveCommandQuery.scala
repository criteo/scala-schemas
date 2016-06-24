package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.queries.fragments.HiveQueryProperties

/**
 * Allows to set the database and query properties during a query.
 *
 * @param database The database in which to execute the command.
 * @param queryProperties Any query-time properties.
 */
case class HiveCommandQuery(database: Option[String] = None,
                            queryProperties: Option[HiveQueryProperties] = None) extends HiveQuery {

  /**
   * Sets the properties, if any, and moves into the database, if any:
   * {{{
   * SET property1=value1;
   * ...
   *
   * USE database;
   * }}}
   *
   * @return The query.
   */
  override def make: String = (database, queryProperties) match {
    case (None, None) => ""
    case (Some(db), None) => s"USE $db;"
    case (None, Some(props)) => props.generate
    case (Some(db), Some(props)) => s"${props.generate}\n\nUSE $db;"
  }

}
