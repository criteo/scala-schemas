package com.criteo.scalaschemas.hive

import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveSchema]].
 */
class HiveSchemaSpec extends FlatSpec with Matchers {
  "A HiveSchema" should "not allow duplicated columns" in {
    val columns = HiveColumns
      .add(HiveColumn("foo", "int"))
      .add(HiveColumn("bar", "int"))
    val partitions = HivePartitionColumns
      .add(HivePartitionColumn("foo", "string"))
      .add(HivePartitionColumn("baz", "string"))

    an[IllegalArgumentException] should be thrownBy
      HiveSchema(
        database = "db",
        table = "table",
        columns = columns,
        hdfsLocation = "/foo/bar",
        partitionColumns = Some(partitions)
      )
  }
}
