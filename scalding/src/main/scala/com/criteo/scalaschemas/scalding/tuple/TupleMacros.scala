package com.criteo.scalaschemas.scalding.tuple

import cascading.tuple.Fields
import com.criteo.scalaschemas.SchemaMacroSupport
import com.twitter.scalding.{TupleConverter, TupleSetter}

import scala.language.experimental.macros
import scala.reflect.macros.Context

/**
  * Utility methods to use Scala types as schemas for scalding and langoustine.
  */
object TupleMacros {

  import SchemaMacroSupport._

  def scaldingFieldsFor[A]: Fields = macro scaldingFieldsForImpl[A]

  def scaldingFieldsForImpl[A: c.WeakTypeTag](c: Context): c.Expr[Fields] = {
    import c.universe._

    val constructorArgs: List[c.universe.Tree] = extractConstructorSymbols[A](c).map { symbol =>
      q"${symbol.name.decoded}"
    }

    c.Expr[Fields] {
      q"new _root_.cascading.tuple.Fields(..$constructorArgs)"
    }
  }

  /**
    * Builds a typed [[TupleConverter]] for scalding workflows.
    *
    * @tparam A the type to generate the converter for
    * @return
    */
  def scaldingTupleConverterFor[A]: TupleConverter[A] = macro scaldingTupleConverterForImpl[A]

  def scaldingTupleConverterForImpl[A: c.WeakTypeTag](c: Context): c.Expr[TupleConverter[A]] = {
    import c.universe._

    val constructorArgs: List[c.universe.Tree] = extractConstructorSymbols[A](c).zipWithIndex.map { case (symbol, pos) =>
      val getter = if (symbol.asTerm.isParamWithDefault) {
        q"""
          _root_.com.criteo.scalaschemas.scalding.tuple.TupleReadWrite.safeReadWrite[${symbol.typeSignature}](
            _root_.com.criteo.scalaschemas.SchemaMacroSupport.getDefaultConstructorArgument[${weakTypeOf[A]}, ${symbol.typeSignature}]($pos)
          ).read(tuple.getObject($pos))
        """
      } else {
        q"implicitly[_root_.com.criteo.scalaschemas.scalding.tuple.TupleReadWrite[${symbol.typeSignature}]].read(tuple.getObject($pos))"
      }
      q"""
        try($getter) catch { case e: _root_.java.lang.Throwable =>
          throw new _root_.java.lang.Exception(
            "Error while extracting " + ${symbol.name.decoded} + " from " + tuple, e
          )
        }
      """
    }

    val tpe = weakTypeOf[A]
    val tupleArity = constructorArgs.size

    c.Expr[TupleConverter[A]] {
      q"""
        new _root_.com.twitter.scalding.TupleConverter[$tpe] {
          override def apply(tuple: _root_.cascading.tuple.TupleEntry): $tpe = {
            new $tpe(..$constructorArgs)
          }
          override val arity: Int = $tupleArity
        }
       """
    }
  }

  /**
    * Generates a typed [[TupleSetter]] for scalding workflows.
    *
    * @tparam A the type to generate the setter for.
    * @return
    */
  def scaldingTupleSetterFor[A]: TupleSetter[A] = macro scaldingTupleSetterForImpl[A]

  def scaldingTupleSetterForImpl[A: c.WeakTypeTag](c: Context): c.Expr[TupleSetter[A]] = {
    import c.universe._

    val constructorSymbols = extractConstructorSymbols[A](c)

    val typeSettingTree: List[c.universe.Tree] = constructorSymbols.zipWithIndex.map {
      case (symbol: Symbol, pos: Int) =>
        q"""
          tuple.set(
            $pos,
            implicitly[_root_.com.criteo.scalaschemas.scalding.tuple.TupleReadWrite[${symbol.typeSignature}]].write(
              arg.${newTermName(symbol.name.decoded)}
            )
          )
        """
    }

    val settingBlock = Block(typeSettingTree, Literal(Constant()))
    val tpe = weakTypeOf[A]
    val tupleArity = constructorSymbols.size
    c.Expr[TupleSetter[A]] {
      q"""
      new _root_.com.twitter.scalding.TupleSetter[$tpe] {
        override def apply(arg: $tpe): _root_.cascading.tuple.Tuple = {
          val tuple = _root_.cascading.tuple.Tuple.size($tupleArity)
          $settingBlock
          tuple
        }
        override val arity: Int = $tupleArity
      }
     """
    }
  }

}
