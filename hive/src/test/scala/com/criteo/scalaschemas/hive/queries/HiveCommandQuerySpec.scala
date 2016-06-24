package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveCommandQuery]]s.
 */
class HiveCommandQuerySpec extends FlatSpec with Matchers with MockitoSugar {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  val database = "foo"
  val props = HiveQueryProperties
    .add(HiveQueryProperty("foo", "bar"))
    .add(HiveQueryProperty("baz", "bim"))

  "A HiveCommandQuery" should "move into the specified database" in {
    val query = HiveCommandQuery(Some(database))
    query.make.compact should equal("USE foo;")
  }

  it should "set any properties" in {
    val query = HiveCommandQuery(queryProperties = Some(props))
    query.make.compact should equal(
      """SET foo=bar;
        |SET baz=bim;
      """.stripMargin.compact)
  }

  it should "move into a database and set any properties" in {
    val query = HiveCommandQuery(Some(database), Some(props))
    query.make.compact should equal(
      """SET foo=bar;
        |SET baz=bim;
        |
        |USE foo;
      """.stripMargin.compact)
  }
}
