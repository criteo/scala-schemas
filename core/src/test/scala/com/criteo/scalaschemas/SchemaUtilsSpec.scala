package com.criteo.scalaschemas

import org.scalatest.{WordSpec, Matchers}

class SchemaUtilsSpec extends WordSpec with Matchers {

  "SchemaUtils" should {
    "extract duplicates from a list" in {
      val elements = Seq(1, 2, 2, 3)

      SchemaUtils.duplicates[Int](elements) shouldEqual Seq(2)
    }

    "extract duplicates based on a custom extraction function" in {
      case class foo(name: String, value: Int)

      val elements = Seq(
        foo("foo1", 1),
        foo("foo2", 2),
        foo("foo2", 3),
        foo("foo3", 0)
      )

      SchemaUtils.duplicates[foo, String](elements)(a => a.name) shouldEqual Seq("foo2")
    }

    "return no duplicates for a list without duplicates" in {
      val elements = Seq(1, 2, 3)

      SchemaUtils.duplicates[Int](elements) shouldEqual Nil
    }

    "compact strings for easy comparison" in {
      import SchemaUtils.StringCompactor

      """
        foo
        bar
      """.compact shouldEqual "foo bar"
    }
  }

}
