package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success, Try}

/**
 * Specifications for [[HiveShowPartitionQuery]]s.
 */
class HiveShowPartitionQuerySpec extends FlatSpec with Matchers with MockitoSugar {

  "A HiveShowPartitionQuery" should "show partitions if no value is specified" in {
    val query = HiveShowPartitionQuery("foo", None)
    query.make should equal("SHOW PARTITIONS foo;")
  }

  it should "show partitions restricted to some partition values if specified" in {
    val partitions = HivePartitionValues
      .add("bar", "baz")
      .add("bim", "bam")
    val query = HiveShowPartitionQuery("foo", Some(partitions))
    query.make should equal("SHOW PARTITIONS foo PARTITION (bar = 'baz', bim = 'bam');")
  }

  it should "not be valid to have missing partition values" in {
    val partitions = HivePartitionValues
      .add("bar", "baz")
      .add("bim", None)
    Try(HiveShowPartitionQuery("foo", Some(partitions))) match {
      case Success(_) => fail()
      case Failure(_) =>
    }
  }
}
