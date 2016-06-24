package com.criteo.scalaschemas.hive

import com.criteo.scalaschemas.examples._
import com.criteo.scalaschemas.hive.queries.fragments.{HiveColumns, HiveColumn}
import org.scalatest.{Matchers, WordSpec}


class HiveMacrosSpec extends WordSpec with Matchers {

  "HiveMacros" should {
    "build a Schema from a case class" in {
      hiveSchemaMatch(
        HiveMacros.toHive[FooFoo]("foo", "hdfsLocation", None),
        HiveSchema(
          table = "foo_foo",
          database = "foo",
          hdfsLocation = "hdfsLocation/foo_foo",
          columns = new HiveColumns(
            HiveColumn("name", "varchar(24)") ::
              HiveColumn("priority", "int") :: Nil
          ),
          partitionColumns = None
        )
      ) shouldEqual true
    }

    "build a Schema from a vanilla class" in {
      hiveSchemaMatch(
        HiveMacros.toHive[Bar]("bar", "hdfsLocation", None),
        HiveSchema(
          table = "bar",
          database = "bar",
          hdfsLocation = "hdfsLocation/bar",
          columns = new HiveColumns(HiveColumn("name", "varchar(32)") :: Nil),
          partitionColumns = None
        )
      ) shouldEqual true
    }

    "build a Schema from a class with all possible types and an unknown" in {
      hiveSchemaMatch(
        HiveMacros.toHive[AllMappings]("all", "hdfsLocation", None),
        HiveSchema(
          table = "all_mappings",
          database = "all",
          hdfsLocation = "hdfsLocation/all_mappings",
          columns = new HiveColumns(
            Seq(
              HiveColumn("someShort", "smallint"),
              HiveColumn("someInt", "int"),
              HiveColumn("someLong", "bigint"),
              HiveColumn("someDateTime", "string"),
              HiveColumn("someFloat", "float"),
              HiveColumn("someDouble", "double"),
              HiveColumn("someBoolean", "boolean"),
              HiveColumn("someString", "varchar(64)")
            )
          ),
          partitionColumns = None
        )
      ) shouldEqual true
    }
  }

  def hiveSchemaMatch(a: HiveSchema, b: HiveSchema): Boolean = {
    a.table == b.table &&
      a.database == b.database &&
      a.hdfsLocation == b.hdfsLocation &&
      a.partitionColumns == b.partitionColumns &&
      a.columns.columns.zipAll(b.columns.columns, HiveColumn(null, null), HiveColumn(null, null)).forall { case (c1, c2) =>
        c1.name == c2.name &&
          c1.typeName == c2.typeName
      }
  }

}

