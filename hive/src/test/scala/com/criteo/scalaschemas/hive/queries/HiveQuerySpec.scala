package com.criteo.scalaschemas.hive.queries

import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveQuery]]s.
 */
class HiveQuerySpec extends FlatSpec with Matchers {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  "followedBy" should "concatenate two queries" in {
    val first = queryStub("SELECT * FROM bar;")
    val second = queryStub("SELECT * FROM foo;")
    first.followedBy(second).make.compact should equal(
      """SELECT * FROM bar;
        |
        |SELECT * FROM foo;
      """.stripMargin.compact)
  }

  def queryStub(query: String) = new HiveQuery {
    override def make = query
  }
}
