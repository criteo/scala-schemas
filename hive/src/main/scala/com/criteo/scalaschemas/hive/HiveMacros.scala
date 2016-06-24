package com.criteo.scalaschemas.hive

import com.criteo.scalaschemas.SchemaColAnnotation
import com.criteo.scalaschemas.hive.queries.fragments.HivePartitionColumns
import scala.annotation.StaticAnnotation
import scala.reflect.macros.Context
import scala.language.experimental.macros

object HiveMacros {
  import com.criteo.scalaschemas.SchemaMacroSupport._

  /**
    * Translates a scala type to a [[HiveSchema]] with Hive compliant types.
    *
    * @param database
    * @tparam A
    * @return
    */
  def toHive[A](database: String, hdfsRoot: String, partitionColumns: Option[HivePartitionColumns]): HiveSchema = macro toHiveImpl[A]

  def toHiveImpl[A: c.WeakTypeTag](c: Context)(database: c.Expr[String], hdfsRoot: c.Expr[String], partitionColumns: c.Expr[Option[HivePartitionColumns]]): c.Expr[HiveSchema] = {
    import c.universe._
    val schema = mkSchema[A](c)(database, mapConstructorArgTypes[A, HiveCol](c)(DefaultHiveSchemaMappers.mkHive10TypeDeclaration))
    c.Expr[HiveSchema] {
      q"""
        val schema = $schema
        _root_.com.criteo.scalaschemas.hive.HiveSchema(
          table = schema.tableName,
          database = schema.database,
          hdfsLocation = $hdfsRoot + "/" + schema.tableName,
          columns = _root_.com.criteo.scalaschemas.hive.queries.fragments.HiveColumns(
            _root_.scala.collection.immutable.Seq(schema.columns.map { col =>
              _root_.com.criteo.scalaschemas.hive.queries.fragments.HiveColumn(
                col.name,
                col.typeName
              )
            }:_*)
          ),
          partitionColumns = $partitionColumns
        )
      """
    }
  }
}

object DefaultHiveSchemaMappers {
  val mkHive10TypeDeclaration: PartialFunction[String, String] = {
    case "Int" => "int"
    case "Short" => "smallint"
    case "Long" => "bigint"
    case "Double" => "double"
    case "Float" => "float"
    case "Boolean" => "boolean"
    case "String" => "string"
    case "org.joda.time.DateTime" => "string"
  }
}

/**
  * Overrides standard mapping behavior for scala to hive types.
  *
  * @param definition a free form definition (eg "varchar(64) -- a series of characters")
  */
case class HiveCol(definition: String) extends StaticAnnotation with SchemaColAnnotation