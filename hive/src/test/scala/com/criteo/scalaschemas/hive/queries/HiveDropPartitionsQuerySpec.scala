package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveDropPartitionsQuery]]s.
 */
class HiveDropPartitionsQuerySpec extends FlatSpec with Matchers with MockitoSugar {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  val schema = HiveSchema(
    database = "db",
    table = "tbl",
    columns = HiveColumns.add(HiveColumn("foo", "int")),
    hdfsLocation = "/home/foo/bar",
    partitionColumns = Some(HivePartitionColumns
      .add(HivePartitionColumn("bar", "string"))
      .add(HivePartitionColumn("baz", "int")))
  )

  val partitionValues1 = HivePartitionValues
    .add(HivePartitionValue("bar", "barbar"))
    .add(HivePartitionValue("baz", "0"))

  val partitionValues2 = HivePartitionValues
    .add(HivePartitionValue("bar", "bim"))
    .add(HivePartitionValue("baz", "1"))

  "A HiveDropPartitionsQuery" should "add one partition with its location when specified" in {
    val query = HiveDropPartitionsQuery(schema, Seq(partitionValues1))
    query.make should equal {
      "ALTER TABLE tbl DROP PARTITION (bar = 'barbar', baz = '0');"
    }
  }

  it should "add two partitions with no location when absent" in {
    val query = HiveDropPartitionsQuery(schema, Seq(partitionValues1, partitionValues2))
    query.make.compact should equal(
      """ALTER TABLE tbl DROP PARTITION (bar = 'barbar', baz = '0');
        |ALTER TABLE tbl DROP PARTITION (bar = 'bim', baz = '1');
      """.stripMargin.compact)
  }
}
