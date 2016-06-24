package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveUDF]] and [[HiveUDFs]].
 */
class HiveUDFSpec extends FlatSpec with Matchers with MockitoSugar {
  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  "A HiveUDF" should "create a UDF" in {
    HiveUDF("foo", "com.company.Foo").generate should equal("CREATE TEMPORARY FUNCTION foo AS 'com.company.Foo';")
  }

  "A HiveUDFs" should "create multiple UDFs" in {
    val udfs = HiveUDFs
      .add(HiveUDF("foo", "com.company.Foo"))
      .add("bar", "com.company.Bar")
    udfs.generate.compact should equal(
      """CREATE TEMPORARY FUNCTION foo AS 'com.company.Foo';
        |CREATE TEMPORARY FUNCTION bar AS 'com.company.Bar';
      """.stripMargin.compact)
  }
}
