package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveAddPartitionsQuery]]s.
 */
class HiveAddPartitionsQuerySpec extends FlatSpec with Matchers with MockitoSugar {

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
  val location1 = s"${schema.hdfsLocation}/somewhere"

  val partitionValues2 = HivePartitionValues
    .add(HivePartitionValue("bar", "bim"))
    .add(HivePartitionValue("baz", "1"))
  val location2 = s"${schema.hdfsLocation}/elsewhere"

  "A HiveAddPartitionsQuery" should "add one partition with its location when specified" in {
    val query = HiveAddPartitionsQuery(schema, Seq(HivePartitionDefinition(partitionValues1, Some(location1))))
    query.make should equal {
      "ALTER TABLE tbl ADD PARTITION (bar = 'barbar', baz = '0') LOCATION '/home/foo/bar/somewhere';"
    }
  }

  it should "add one partition with no location when absent" in {
    val query = HiveAddPartitionsQuery(schema, Seq(HivePartitionDefinition(partitionValues1)))
    query.make should equal {
      "ALTER TABLE tbl ADD PARTITION (bar = 'barbar', baz = '0');"
    }
  }

  it should "add two partition with their locations when specified" in {
    val query = HiveAddPartitionsQuery(schema, Seq(
      HivePartitionDefinition(partitionValues1, Some(location1)),
      HivePartitionDefinition(partitionValues2, Some(location2))
    ))
    query.make.compact should equal(
      """ALTER TABLE tbl ADD PARTITION (bar = 'barbar', baz = '0') LOCATION '/home/foo/bar/somewhere';
        |ALTER TABLE tbl ADD PARTITION (bar = 'bim', baz = '1') LOCATION '/home/foo/bar/elsewhere';
      """.stripMargin.compact)
  }

  it should "add two partitions with no location when absent" in {
    val query = HiveAddPartitionsQuery(schema, Seq(
      HivePartitionDefinition(partitionValues1),
      HivePartitionDefinition(partitionValues2)
    ))
    query.make.compact should equal(
      """ALTER TABLE tbl ADD PARTITION (bar = 'barbar', baz = '0');
        |ALTER TABLE tbl ADD PARTITION (bar = 'bim', baz = '1');
      """.stripMargin.compact)
  }
}
