package com.criteo.scalaschemas

import scala.reflect.macros.Context
import scala.language.experimental.macros

object SchemaMacroSupport {

  /**
    * Maps the primary constructor argument types to the target type.
    *
    * @param c
    * @param typeMapper the mapping function to use
    * @tparam A the source type
    * @tparam B the target type annotation class
    * @return
    */
  def mapConstructorArgTypes[A: c.WeakTypeTag, B: c.TypeTag](c: Context)(typeMapper: PartialFunction[String, String]): List[(String, String)] = {
    import c.universe._

    val liftedMapper = typeMapper.lift

    extractConstructorSymbols[A](c).map { symbol =>
      val symbolName = symbol.name.decoded
      val symbolType = symbol.typeSignature.toString

      val maybeAnnotation: Option[String] = symbol.annotations.collectFirst { case a if a.tpe =:= typeOf[B] =>
        a.scalaArgs.head match {
          case Literal(Constant(v)) => v.toString
          case x => c.abort(c.enclosingPosition, "Annotation requires String literal.")
        }
      }
      val targetType = maybeAnnotation.orElse(liftedMapper(symbolType)).getOrElse(
        c.abort(c.enclosingPosition, s"No type mapping found for $symbolType. You must annotate the field $symbolName with @${typeOf[B]}")
      )
      (symbolName, targetType)
    }
  }

  /**
    * Extracts symbols from the primary constructor and verifies that all conditions are met for later type mapping.
    *
    * @param c
    * @tparam A the source type to parse
    * @return
    */
  def extractConstructorSymbols[A: c.WeakTypeTag](c: Context): List[c.universe.Symbol] = {
    import c.universe._

    val tpe = weakTypeTag[A].tpe

    val constructor = tpe.members.collectFirst { case m: MethodSymbol if m.isPrimaryConstructor => m }.getOrElse(
      c.abort(c.enclosingPosition, "No constructor found!  You must specify a concrete class or case class!")
    )

    val constructorArgs: List[c.universe.Symbol] = constructor.paramss.head

    if (constructorArgs.size < 1)
      c.abort(c.enclosingPosition, s"Primary constructor for type [$tpe] has no args.")

    val getters: Set[String] = tpe.members.collect { case m: MethodSymbol if m.isGetter => m.name.decoded }(collection.breakOut)

    val nonGetters = constructorArgs.filterNot { symbol: Symbol => getters.contains(symbol.name.decoded) }
    if (nonGetters.nonEmpty)
      c.abort(c.enclosingPosition, s"Illegal constructor, all args must be vals or vars, these are not: $nonGetters")

    constructorArgs
  }

  def mkSchema[A: c.WeakTypeTag](c: Context)(database: c.Expr[String], params: List[(String, String)]): c.Expr[Schema] = {
    import c.universe._
    val tpeName = weakTypeTag[A].tpe.typeSymbol.name.decoded
    val tableName = """([A-Z])|(\d+)""".r.replaceAllIn(tpeName, '_' + _.matched.toLowerCase).dropWhile(_ == '_')
    val columnsTree = params.map { case (name, tpe) =>
      q"_root_.com.criteo.scalaschemas.Column($name, $tpe)"
    }
    c.Expr[Schema] {
      q"""
        new _root_.com.criteo.scalaschemas.Schema(
          database = $database,
          tableName = $tableName,
          columns = _root_.scala.collection.immutable.List(..$columnsTree)
        )
      """
    }
  }

  private val defaultsCache = scala.collection.concurrent.TrieMap.empty[String,java.lang.reflect.Method]

  def getDefaultConstructorArgument[A: Manifest, B](pos: Int): B = {
    val method = {
      val k = classManifest[A].runtimeClass + "$lessinit$greater$default$" + (pos + 1)

      defaultsCache.getOrElse(k, {
        val m = scala.reflect.classTag[A].runtimeClass.getMethod("$lessinit$greater$default$" + (pos + 1))
        defaultsCache += (k -> m)
        m
      })
    }
    method.invoke(null).asInstanceOf[B]
  }

  def coerce[A](x: Any, to: Class[_])(expr: PartialFunction[Any, A]): A = {
    try expr(x) catch { case e: Throwable =>
      throw new Exception(
        s"Cannot coerce value `$x` to type $to", e
      )
    }
  }
}

case class Schema(database: String, tableName: String, schemaName: Option[String] = None, columns: Seq[Column])

case class Column(name: String, typeName: String)

trait SchemaColAnnotation {
  def definition: String
}
