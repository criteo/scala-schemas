package com.criteo.scalaschemas.hive.queries.fragments

import org.scalatest.mock.MockitoSugar
import org.scalatest.{ShouldMatchers, WordSpec}

/**
  * Specifications for [[HivePartitionValue]], [[HivePartitionValues]] and [[HivePartitionDefinition]].
  */
class HivePartitionValueSpec extends WordSpec with ShouldMatchers with MockitoSugar {

  "A HivePartitionValue" should {
    "generate a partition value" in {
      HivePartitionValue("foo", "bar").generate shouldEqual "foo = 'bar'"
    }

    "not include the value if it is absent" in {
      HivePartitionValue("foo", None).generate shouldEqual "foo"
    }
  }

  "A HivePartitionValues" should {
    "generate a partition" in {
      val partition = HivePartitionValues
        .add("foo", "bar")
        .add(HivePartitionValue("bam", "baz"))
        .add("bim", None)
        .generate

      partition shouldEqual "PARTITION (foo = 'bar', bam = 'baz', bim)"
    }

    "not allow duplicated column names" in {
      val partition = HivePartitionValues
        .add(HivePartitionValue("foo", "foofoo"))
        .add(HivePartitionValue("bar", "barbar"))

      an[IllegalArgumentException] should be thrownBy
        partition.add(HivePartitionValue("foo", "foofoofoo"))
    }

    "be generated properly from the result of a SHOW PARTITIONS" in {
      HivePartitionValues.fromString("day=2016-01-20/hour=03/host_platform=AS") shouldEqual
        Some(HivePartitionValues
          .add("day", "2016-01-20")
          .add("hour", "03")
          .add("host_platform", "AS"))
      HivePartitionValues.fromString("") shouldEqual None
    }

    "be equivalent to another partition even if their order is different" in {
      val p1 = HivePartitionValues
        .add("day", "2016-01-20")
        .add("hour", "03")
        .add("host_platform", "AS")

      val p2 = HivePartitionValues
        .add("hour", "03")
        .add("day", "2016-01-20")
        .add("host_platform", "AS")

      p1.isEquivalentTo(p2) shouldBe true
    }
  }

  "A HivePartitionDefinition" should {
    "generate a partition with its location when specified" in {
      HivePartitionDefinition(HivePartitionValues.add("foo", "bar"), Some("/some/location")).generate shouldEqual
        "PARTITION (foo = 'bar') LOCATION '/some/location'"
    }

    "omit the location when no location is specified" in {
      HivePartitionDefinition(HivePartitionValues.add("foo", "bar")).generate shouldEqual "PARTITION (foo = 'bar')"
    }
  }
}
