package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveAddJar]] and [[HiveAddJars]].
 */
class HiveAddJarSpec extends FlatSpec with Matchers with MockitoSugar {

  "A HiveAddJar" should "add a jar" in {
    HiveAddJar("/some/location.jar").generate should equal("ADD JAR '/some/location.jar';")
  }

  "A HiveAddJars" should "add multiple jars" in {
    val jars = HiveAddJars
      .add(HiveAddJar("/some/location.jar"))
      .add("/other/location.jar")
    jars.generate should equal("ADD JAR '/some/location.jar';\nADD JAR '/other/location.jar';")
  }
}
