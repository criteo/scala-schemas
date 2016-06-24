package com.criteo.scalaschemas.hive.queries

import com.criteo.scalaschemas.hive.HiveSchema
import com.criteo.scalaschemas.hive.queries.fragments._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/**
 * Specifications for [[HiveCreateTableQuery]]s.
 */
class HiveCreateTableQuerySpec extends FlatSpec with Matchers with MockitoSugar {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  val baseSchema = HiveSchema(
    database = "foo",
    table = "bar",
    columns = HiveColumns.add(HiveColumn("baz", "int")),
    hdfsLocation = "/home/foo/bar"
  )

  "A HiveCreateTableQuery" should "build a CREATE TABLE statement" in {
    HiveCreateTableQuery(baseSchema).make.compact should equal {
      """CREATE TABLE IF NOT EXISTS bar
        |(`baz` int)
        |STORED AS TEXTFILE
        |LOCATION '/home/foo/bar'
        |;""".stripMargin.compact
    }
  }

  it should "build an EXTERNAL CREATE TABLE statement" in {
    val schema = baseSchema.copy(external = true)
    HiveCreateTableQuery(schema).make.compact should equal {
      """CREATE EXTERNAL TABLE IF NOT EXISTS bar
        |(`baz` int)
        |STORED AS TEXTFILE
        |LOCATION '/home/foo/bar'
        |;""".stripMargin.compact
    }
  }

  it should "build an CREATE TABLE statement with partitions" in {
    val schema = baseSchema.copy(
      partitionColumns = Some(HivePartitionColumns.add(HivePartitionColumn("boz", "int"))),
      external = true
    )
    new HiveCreateTableQuery(schema).make.compact should equal {
      """CREATE EXTERNAL TABLE IF NOT EXISTS bar
        |(`baz` int)
        |PARTITIONED BY (boz int)
        |STORED AS TEXTFILE
        |LOCATION '/home/foo/bar'
        |;""".stripMargin.compact
    }
  }

  it should "build an CREATE TABLE statement with storage engine" in {
    val schema = baseSchema.copy(
      partitionColumns = Some(HivePartitionColumns.add(HivePartitionColumn("boz", "int"))),
      external = true,
      storageEngine = RCFileHiveStorageFormat
    )
    new HiveCreateTableQuery(schema).make.compact should equal {
      """CREATE EXTERNAL TABLE IF NOT EXISTS bar
        |(`baz` int)
        |PARTITIONED BY (boz int)
        |STORED AS RCFILE
        |LOCATION '/home/foo/bar'
        |;""".stripMargin.compact
    }
  }

  it should "build an CREATE TABLE statement with table properties" in {
    val schema = baseSchema.copy(
      partitionColumns = Some(HivePartitionColumns.add(HivePartitionColumn("boz", "int"))),
      tableProperties = Some(HiveTableProperties.add(HiveTableProperty("a", "b"))),
      external = true
    )
    new HiveCreateTableQuery(schema).make.compact should equal {
      """CREATE EXTERNAL TABLE IF NOT EXISTS bar
        |(`baz` int)
        |PARTITIONED BY (boz int)
        |STORED AS TEXTFILE
        |LOCATION '/home/foo/bar'
        |TBLPROPERTIES ('a'='b');""".stripMargin.compact
    }
  }
}
