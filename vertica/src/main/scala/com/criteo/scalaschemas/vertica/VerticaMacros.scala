package com.criteo.scalaschemas.vertica

import com.criteo.scalaschemas.{SchemaColAnnotation, Schema}
import com.criteo.scalaschemas.SchemaMacroSupport._

import scala.annotation.StaticAnnotation
import scala.reflect.macros.Context
import scala.language.experimental.macros


object VerticaMacros {

  /**
    * Translates a scala type to a [[Schema]] with Vertica compliant types.
    *
    * @param database
    * @tparam A
    * @return
    */
  def toVertica[A](database: String, dataOrganization: VerticaDataOrganization): VerticaTable = macro toVerticaImpl[A]

  def toVerticaImpl[A: c.WeakTypeTag](c: Context)(database: c.Expr[String], dataOrganization: c.Expr[VerticaDataOrganization]): c.Expr[VerticaTable] = {
    import c.universe._
    val schema = mkSchema[A](c)(database, mapConstructorArgTypes[A, VerticaCol](c)(DefaultVerticaSchemaMappers.mkVerticaTypeDeclaration))
    c.Expr[VerticaTable] {
      q"""
        val schema = $schema
        _root_.com.criteo.scalaschemas.vertica.VerticaTable(
          schema = schema.database,
          name = schema.tableName,
          columns = new _root_.com.criteo.scalaschemas.vertica.VerticaColumns(
            _root_.scala.collection.immutable.Seq(schema.columns.map { col =>
              _root_.com.criteo.scalaschemas.vertica.VerticaColumn(
                col.name,
                col.typeName
              )
            }:_*)
          ),
          dataOrganization = $dataOrganization
        )
      """
    }
  }

}

object DefaultVerticaSchemaMappers {

  val mkVerticaTypeDeclaration: PartialFunction[String, String] = {
    case "Int" => "int"
    case "Short" => "smallint"
    case "Long" => "bigint"
    case "Double" => "float"
    case "Float" => "float"
    case "Boolean" => "boolean"
    case "org.joda.time.DateTime" => "datetime"
  }
}

/**
  * Overrides standard mapping behavior for scala to vertica types.
  *
  * NOTE: you'll probably always want to use this for certain types (ie String)!!!
  *
  * @param definition a free form definition (eg "varchar(64) encoding Dict")
  */
case class VerticaCol(definition: String) extends StaticAnnotation with SchemaColAnnotation