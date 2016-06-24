package com.criteo.scalaschemas.vertica

import com.criteo.scalaschemas.examples.AllMappings
import org.scalatest.{WordSpec, Matchers}


class VerticaMacrosSpec extends WordSpec with Matchers {

  "VerticaMacros" should {

    "build a Schema with Vertica types" in {
      verticalTableMatch(
        VerticaMacros.toVertica[AllMappings]("all", VerticaDataOrganization(None, None)),
        VerticaTable(
          schema = "all",
          name = "all_mappings",
          columns = new VerticaColumns(
            Seq(
              VerticaColumn("someShort", "smallint"),
              VerticaColumn("someInt", "int"),
              VerticaColumn("someLong", "bigint"),
              VerticaColumn("someDateTime", "datetime"),
              VerticaColumn("someFloat", "float"),
              VerticaColumn("someDouble", "float"),
              VerticaColumn("someBoolean", "boolean"),
              VerticaColumn("someString", "varchar(128)")
            )
          ),
          dataOrganization = VerticaDataOrganization(None, None)
        )
      ) shouldEqual true
    }
  }

  def verticalTableMatch(a: VerticaTable, b: VerticaTable): Boolean = {
    a.name == b.name &&
      a.schema == b.schema &&
      a.dataOrganization == b.dataOrganization &&
      a.columns.columns.zipAll(b.columns.columns, VerticaColumn(null, null), VerticaColumn(null, null)).forall { case (c1, c2) =>
        c1.name == c2.name &&
          c1.dataType == c2.dataType
      }
  }

}
