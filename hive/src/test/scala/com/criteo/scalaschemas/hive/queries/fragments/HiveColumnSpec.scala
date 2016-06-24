package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveColumn]] and [[HiveColumns]].
 */
class HiveColumnSpec extends FlatSpec with Matchers with MockitoSugar {

  "A HiveColumn" should "generate a column definition" in {
    HiveColumn("foo", "bar").generate should equal("`foo` bar")
  }

  it should "generate a column with a comment" in {
    HiveColumn("foo", "bar", Some("baz")).generate should equal("`foo` bar COMMENT 'baz'")
  }

  "A HiveColumns" should "generate a list columns" in {
    val columns = HiveColumns
      .add("foo", "bar")
      .add(HiveColumn("bam", "baz", Some("bim")))
      .generate

    columns should equal(
      s"""(`foo` bar,
         |`bam` baz COMMENT 'bim')""".stripMargin
    )
  }

  it should "not allow duplicated column names" in {
    val columns = HiveColumns
      .add(HiveColumn("foo", "int"))
      .add(HiveColumn("bar", "int"))

    an[IllegalArgumentException] should be thrownBy
      columns.add(HiveColumn("foo", "string"))
  }
}
