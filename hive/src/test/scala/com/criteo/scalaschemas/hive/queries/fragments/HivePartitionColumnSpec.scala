package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HivePartitionColumn]] and [[HivePartitionColumns]].
 */
class HivePartitionColumnSpec extends FlatSpec with Matchers with MockitoSugar {

  "A HivePartitionColumn" should "generate a partition column" in {
    HivePartitionColumn("foo", "bar").generate should equal("foo bar")
  }

  it should "generate a partition column with a comment" in {
    HivePartitionColumn("foo", "bar", Some("bam")).generate should equal("foo bar COMMENT 'bam'")
  }

  "A HivePartitionColumns" should "generate a list of partition columns" in {
    val partitions = HivePartitionColumns
      .add("foo", "bar")
      .add(HivePartitionColumn("bam", "baz", Some("bim")))
      .generate

    partitions should equal(
      s"""PARTITIONED BY (foo bar,
         |bam baz COMMENT 'bim')""".stripMargin
    )
  }

  it should "not allow duplicated column names" in {
    val partitions = HivePartitionColumns
      .add(HivePartitionColumn("foo", "int"))
      .add(HivePartitionColumn("bar", "int"))

    an[IllegalArgumentException] should be thrownBy
      partitions.add(HivePartitionColumn("foo", "string"))
  }
}
