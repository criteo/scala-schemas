package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveOutputQuery]]s.
 */
class HiveOutputQuerySpec extends FlatSpec with Matchers {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  val schema = HiveSchema(
    database = "foo",
    table = "bar",
    columns = HiveColumns.add(HiveColumn("baz", "int")),
    hdfsLocation = "/home/foo/bar"
  )
  val query = s"SELECT * from ${schema.table}"
  val partition = HivePartitionValues.add(HivePartitionValue("baz", "1"))

  "A HiveOutputQuery" should "INSERT in a specific partition of the table if specified" in {
    HiveOutputQuery(schema, query, Some(partition)).make.compact should equal {
      """INSERT OVERWRITE TABLE bar
        |PARTITION (baz = '1')
        |SELECT * from bar;""".stripMargin.compact
    }
  }

  it should "INSERT directly in the table if no partition is specified" in {
    HiveOutputQuery(schema, query, None).make.compact should equal {
      """INSERT OVERWRITE TABLE bar
        |SELECT * from bar;""".stripMargin.compact
    }
  }
}
