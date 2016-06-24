package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveQueryProperty]] and [[HiveQueryProperties]].
 */
class HiveQueryPropertySpec extends FlatSpec with Matchers with MockitoSugar {

  "A HiveQueryProperty" should "generate a query property" in {
    HiveQueryProperty("foo", "bar").generate should equal("SET foo=bar;")
  }

  "A HiveQueryProperties" should "generate a list of SET properties" in {
    val tblProps = HiveQueryProperties
      .add("foo", "bar")
      .add(HiveQueryProperty("bam", "baz"))
      .generate

    tblProps should equal(
      s"""SET foo=bar;
         |SET bam=baz;""".stripMargin
    )
  }

  it should "generate as list of --hiveconf properties" in {
    val cliProps = HiveQueryProperties
      .add(HiveQueryProperty("foo", "bar"))
      .add(HiveQueryProperty("bam", "baz"))
      .cliArguments

    cliProps should equal(Seq(
      "--hiveconf", "foo=bar",
      "--hiveconf", "bam=baz"
    ))
  }
}
