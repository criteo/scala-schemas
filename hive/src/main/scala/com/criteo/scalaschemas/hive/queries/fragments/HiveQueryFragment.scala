package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A trait for Hive Query Fragments, which we'll use to build up full Hive SQL statements.
 */
trait HiveQueryFragment {

  /**
   * @return A Hive SQL fragment.
   */
  def generate: String

}
