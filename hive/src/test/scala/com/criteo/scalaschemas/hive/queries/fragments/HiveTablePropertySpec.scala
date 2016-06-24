package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveTableProperty]] and [[HiveTableProperties]].
 */
class HiveTablePropertySpec extends FlatSpec with Matchers with MockitoSugar {

  "A HiveTableProperty" should "generate a key value pair" in {
    HiveTableProperty("foo", "bar").generate should equal("'foo'='bar'")
  }

  "A HiveTableProperties" should "generate a list of key value pairs" in {
    val tblProps = HiveTableProperties
      .add("foo", "bar")
      .add(HiveTableProperty("bam", "baz"))
      .generate

    tblProps should equal(
      s"""TBLPROPERTIES ('foo'='bar',
         |'bam'='baz')""".stripMargin
    )
  }
}
