package com.criteo.scalaschemas.vertica

import org.scalatest.{FlatSpec, Matchers}

/**
  * Specs for [[VerticaTable]] and the classes it uses.
  */
class VerticaTableSpec extends FlatSpec with Matchers {

  import com.criteo.scalaschemas.SchemaUtils.StringCompactor

  val table = VerticaTable(
    schema = "sch",
    name = "tbl",
    columns = VerticaColumns.add("foo", "INT").add("bar", "FLOAT"),
    dataOrganization = VerticaDataOrganization(None, None)
  )

  "A VerticaTableConstraint" should "generate a named table constraint" in {
    val constraint = VerticaTableConstraint("PRIMARY KEY(a,b)", "PK_foo")
    constraint.make should equal("CONSTRAINT PK_foo PRIMARY KEY(a,b)")
  }

  "A VerticaColumnConstraint" should "generate an unnamed constraint if no name is given" in {
    val constraint = VerticaColumnConstraint("UNIQUE")
    constraint.make should equal("UNIQUE")
  }

  it should "generate a named constraint if a name is given" in {
    val constraint = VerticaColumnConstraint("UNIQUE", Some("foo_unique"))
    constraint.make should equal("CONSTRAINT foo_unique UNIQUE")
  }

  "A VerticaColumn" should "generate a simple column definition" in {
    val col = VerticaColumn("foo", "INT")
    col.tableColumn should equal("foo INT")
    col.projectionColumn should equal("foo")
  }

  it should "add encoding if present" in {
    val col = VerticaColumn("foo", "INT", encoding = Some("RLE"))
    col.tableColumn should equal("foo INT ENCODING RLE")
    col.projectionColumn should equal("foo ENCODING RLE")
  }

  it should "add any constraints" in {
    val col = VerticaColumn("foo", "INT", columnConstraint = Some(VerticaColumnConstraint("NOT NULL")))
    col.tableColumn should equal("foo INT NOT NULL")
    col.projectionColumn should equal("foo")
  }

  it should "generate a complex column definition" in {
    val col = VerticaColumn("foo", "INT", Some(VerticaColumnConstraint("NOT NULL")), Some("RLE"), Some(5))
    col.tableColumn should equal("foo INT NOT NULL ENCODING RLE ACCESSRANK 5")
    col.projectionColumn should equal("foo ENCODING RLE ACCESSRANK 5")
  }

  "A VerticaColumns" should "generate the definition of multiple columns" in {
    val cols = VerticaColumns.add("foo", "INT").add("bar", "CHAR(5)")
    cols.tableColumns should equal("foo INT,\nbar CHAR(5)")
    cols.projectionColumns should equal("foo,\nbar")
  }

  it should "disallow duplicated column names" in {
    val cols = VerticaColumns.add("foo", "INT")
    intercept[IllegalArgumentException] {
      cols.add("foo", "FLOAT")
    }
  }

  "A VerticaDataOrganization" should "generate the organization of a table or projection" in {
    val org = VerticaDataOrganization(None, None, 4)
    org.make should equal("UNSEGMENTED ALL NODES KSAFE 4")
  }

  it should "generate a 'ORDER BY' clause if needed" in {
    val org = VerticaDataOrganization(Some(List("foo", "bar")), None)
    org.make should equal("ORDER BY foo, bar UNSEGMENTED ALL NODES KSAFE 1")
  }

  it should "generate a 'UNSEGMENTED BY' clause if needed" in {
    val org = VerticaDataOrganization(Some(List("foo", "bar")), Some(List("bar", "baz")))
    org.make should equal("ORDER BY foo, bar SEGMENTED BY HASH(bar, baz) ALL NODES KSAFE 1")
  }

  it should "validate its columns against a table's columns" in {
    val org = VerticaDataOrganization(Some(List("foo", "bar")), Some(List("bar", "baz")))
    val partialColumns = VerticaColumns.add("bar", "INT").add("baz", "FLOAT")

    intercept[IllegalArgumentException] {
      org.validateAgainstColumns(partialColumns)
    }
    org.validateAgainstColumns(partialColumns.add("foo", "INT"))
  }

  "A VerticaTable" should "create a table" in {
    table.createQuery.compact should equal(
      """CREATE TABLE IF NOT EXISTS sch.tbl (
          foo INT,
          bar FLOAT
        ) UNSEGMENTED ALL NODES KSAFE 1""".compact)
  }

  it should "verify that it has at least one column" in {
    intercept[IllegalArgumentException] {
      table.copy(
        columns = VerticaColumns
      )
    }
  }

  it should "verify that its data organization is valid" in {
    intercept[IllegalArgumentException] {
      table.copy(
        dataOrganization = VerticaDataOrganization(Some(List("bar", "baz")), None)
      )
    }
  }

  it should "add any table constraints" in {
    val tbl = table.copy(
      tableConstraints = List(VerticaTableConstraint("PRIMARY KEY(foo, bar)", "PK_tbl"))
    )
    tbl.createQuery.compact should equal(
      """CREATE TABLE IF NOT EXISTS sch.tbl (
           foo INT,
           bar FLOAT,
           CONSTRAINT PK_tbl PRIMARY KEY(foo, bar)
           ) UNSEGMENTED ALL NODES KSAFE 1""".compact)
  }

  it should "add a partition definition if any" in {
    val tbl = table.copy(
      partitionClause = Some("foo")
    )
    tbl.createQuery.compact should equal(
      """CREATE TABLE IF NOT EXISTS sch.tbl (
           foo INT,
           bar FLOAT
           ) UNSEGMENTED ALL NODES KSAFE 1 PARTITION BY foo""".compact)
  }
}
