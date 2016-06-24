package com.criteo.scalaschemas.hive.queries

/**
 * An encapsulation of a Hive query.
 */
trait HiveQuery {

  /**
   * The query returned should end with a semicolon.
   *
   * @return The fully built, ready to execute query.
   */
  def make: String

  /**
   * Concatenates two queries.
   *
   * @param other Another hive query to concatenate to this one.
   * @return A query corresponding to executing this query, then the other query, in sequence.
   */
  def followedBy(other: HiveQuery): HiveQuery = {
    new HiveQuery {
      override def make: String =
        s"""${HiveQuery.this.make}
            |
            |${other.make}""".stripMargin

      override def toString: String = s"HiveQuery(${HiveQuery.this.toString} followedBy ${other.toString}"
    }
  }

}
