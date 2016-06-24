package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.queries.fragments.{HiveAddJars, HiveUDFs}

/**
 * Allows to add the specified resources during a query.
 *
 * @param jars Any jars to add.
 * @param udfs Any UDFs to create.
 */
case class HiveResourceQuery(jars: Option[HiveAddJars] = None,
                             udfs: Option[HiveUDFs] = None) extends HiveQuery {

  /**
   * First adds the jars, then create the udfs. (Because the udfs might be in the jars)
   */
  override def make: String = (jars, udfs) match {
    case (None, None) => ""
    case (Some(j), None) => j.generate
    case (None, Some(u)) => u.generate
    case (Some(j), Some(u)) => s"${j.generate}\n\n${u.generate}"
  }
}
