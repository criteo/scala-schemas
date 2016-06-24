package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveResourceQuery]]s.
 */
class HiveResourceQuerySpec extends FlatSpec with Matchers with MockitoSugar {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  val jars = HiveAddJars
    .add("/foo/bar.jar")
    .add("/bar/foo.jar")
  val udfs = HiveUDFs
    .add("foo", "com.company.Foo")
    .add("bar", "com.company.Bar")

  "A HiveResourceQuery" should "add the specified jars" in {
    val query = HiveResourceQuery(jars = Some(jars))
    query.make.compact should equal(
      """ADD JAR '/foo/bar.jar';
        |ADD JAR '/bar/foo.jar';
      """.stripMargin.compact)
  }

  it should "create the specified functions" in {
    val query = HiveResourceQuery(udfs = Some(udfs))
    query.make.compact should equal(
      """CREATE TEMPORARY FUNCTION foo AS 'com.company.Foo';
        |CREATE TEMPORARY FUNCTION bar AS 'com.company.Bar';
      """.stripMargin.compact)
  }

  it should "add the specified jars and create the specified functions" in {
    val query = HiveResourceQuery(jars = Some(jars), udfs = Some(udfs))
    query.make.compact should equal(
      """ADD JAR '/foo/bar.jar';
        |ADD JAR '/bar/foo.jar';
        |
        |CREATE TEMPORARY FUNCTION foo AS 'com.company.Foo';
        |CREATE TEMPORARY FUNCTION bar AS 'com.company.Bar';
      """.stripMargin.compact)
  }
}
