package com.criteo.scalaschemas.scalding.parquet

import com.criteo.scalaschemas.SchemaMacroSupport
import com.twitter.scalding.parquet.tuple.scheme.ParquetTupleConverter
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType

import scala.language.experimental.macros
import scala.reflect.macros._

/**
  * Macros for generating type specific Parquet schema, read and write support classes.
  *
  */
object ParquetMacros {

  import SchemaMacroSupport._

  /**
    * Creates the Parquet schema as a [[String]] needed by scalding-parquet support classes.
    *
    * @tparam A
    * @return
    */
  def rootSchemaFor[A]: String = macro rootSchemaForImpl[A]

  /**
    * Creates the Parquet schema fragment in the Parquet native [[MessageType]] class.  You'll use this to make your
    * nested complex-types work with your Parquet jobs.
    * @tparam A
    * @return
    */
  def messageTypeFor[A]: MessageType = macro messageTypeForImpl[A]

  /**
    * Creates a function that writes the root-level record for your type.  The difference between this and [[writeRecordFor]]
    * is simply that here we wrap the internal call in startMessage() and endMessage() as required by Parquet to actually
    * write the record out.
    * @tparam A
    * @return
    */
  def writeMessageFor[A]: (A, RecordConsumer, MessageType) => Unit = macro writeMessageForImpl[A]

  /**
    * Creates a function that writes a leaf of a record for the given type.  Basically this writes a Parquet group for the
    * supplied type.  Use [[writeMessageFor]] for the top-level class in your type's hierarchy!
    * @tparam A
    * @return
    */
  def writeRecordFor[A]: (A, RecordConsumer, MessageType) => Unit = macro writeRecordForImpl[A]

  /**
    * Creates a Parquet to your type converter for the supplied type.  This ends up delegating implicitly to the
    * [[StrictConverterProvider]]s available in scope for the types in your type (phew).  Primitive types have default
    * implementations available in the [[StrictConverterProvider]] companion object.  If your type contains complex
    * types, you'll need to make a [[StrictConverterProvider]] implicitly available for it by calling
    * [[converterProviderFor]] on it in the correct scope.
    * @tparam A
    * @return
    */
  def parquetTupleConverterFor[A]: ParquetTupleConverter[A] = macro parquetTupleConverterForImpl[A]

  /**
    * Creates a [[StrictConverterProvider]] for a given type.  Used for reading nested complex types in Parquet schemas.
    * @tparam A
    * @return
    */
  def converterProviderFor[A]: com.criteo.scalaschemas.scalding.parquet.StrictConverterProvider[A] = macro converterProviderForImpl[A]

  /**
    * Creates a Parquet message for the supplied type [[A]] implicitly using
    * [[com.twitter.scalding.parquet.tuple.scheme.ParquetWriteSupport]]s to translate
    * a Scala type to the type fragment in the message body.
    *
    * eg:
    * message MyType {
    * required int64 some_id;
    * required int32 a_date (DATE);
    * required int32 a_time (TIME);
    * optional double amount;
    * required group user_info {
    * optional int64 cust_id;
    * optional binary device (UTF8);
    * optional binary state (UTF8);
    * }
    * optional group marketing_info {
    * optional int64 camp_id;
    * repeated binary keywords (UTF8);
    * }
    * optional group trans_info {
    * repeated int64 prod_id;
    * optional binary purch_flag (UTF8);
    * }
    * }
    *
    * @param c
    * @tparam A
    * @return
    */
  def messageTypeForImpl[A: c.WeakTypeTag](c: Context): c.Expr[_root_.parquet.schema.MessageType] = {
    c.Expr[_root_.parquet.schema.MessageType] {
      makeMessageTypeFor[A](c)
    }
  }

  def rootSchemaForImpl[A: c.WeakTypeTag](c: Context): c.Expr[String] = {
    import c.universe._

    val messageType = makeMessageTypeFor[A](c)
    c.Expr[String] {
      q"$messageType.toString"
    }
  }

  def makeMessageTypeFor[A: c.WeakTypeTag](c: Context) = {
    import c.universe._

    val tpe = weakTypeOf[A]

    val types: List[c.universe.Tree] = extractConstructorSymbols[A](c).map {
      case symbol =>
        q"implicitly[_root_.com.criteo.scalaschemas.scalding.parquet.ParquetSchemaSupport[${symbol.typeSignature}]].forField(${symbol.name.decoded}, _root_.parquet.schema.Type.Repetition.REQUIRED)"
    }

    val messageType: c.universe.Tree =
      q"""
      new _root_.parquet.schema.MessageType(
        ${tpe.typeSymbol.name.encoded},
        _root_.scala.Array.apply[_root_.parquet.schema.Type](..$types):_*
      )
      """
    messageType
  }

  def writeMessageForImpl[A: c.WeakTypeTag](c: Context): c.Expr[(A, RecordConsumer, MessageType) => Unit] = {
    import c.universe._

    val writeRecord = writeRecordForImpl[A](c)

    val tpe = weakTypeOf[A]

    c.Expr[(A, RecordConsumer, MessageType) => Unit] {
      q"""
      new _root_.scala.Function3[$tpe, RecordConsumer, _root_.parquet.schema.MessageType, Unit] {
        override def apply(in: $tpe, out: RecordConsumer, schema: _root_.parquet.schema.MessageType): Unit = {
         out.startMessage()
         $writeRecord.apply(in, out, schema)
         out.endMessage()
        }
      }
     """
    }
  }

  def writeRecordForImpl[A: c.WeakTypeTag](c: Context): c.Expr[(A, RecordConsumer, MessageType) => Unit] = {
    import c.universe._

    val tpe = weakTypeOf[A]

    val constructorSymbols = extractConstructorSymbols[A](c)

    val typeSettingTree: List[c.universe.Tree] = constructorSymbols.zipWithIndex.map {
      case (symbol: Symbol, pos: Int) =>
        q"""
            out.startField(${symbol.name.decoded}, $pos)
            implicitly[_root_.com.criteo.scalaschemas.scalding.parquet.ParquetWriteRecordSupport[${symbol.typeSignature}]].writeRecord(
              in.${newTermName(symbol.name.decoded)}, out, schema
            )
            out.endField(${symbol.name.decoded}, $pos)
        """
    }

    val settingBlock = Block(typeSettingTree, Literal(Constant()))
    c.Expr[(A, RecordConsumer, MessageType) => Unit] {
      q"""
      new _root_.scala.Function3[$tpe, RecordConsumer, _root_.parquet.schema.MessageType, Unit] {
        override def apply(in: $tpe, out: RecordConsumer, schema: _root_.parquet.schema.MessageType): Unit = {
          $settingBlock
        }
      }
     """
    }

  }

  def parquetTupleConverterForImpl[A: c.WeakTypeTag](c: Context): c.Expr[ParquetTupleConverter[A]] = {
    import c.universe._
    val converterProvider = converterProviderForImpl[A](c)

    c.Expr[ParquetTupleConverter[A]] {
      q"$converterProvider.makeFor(None).asParquetTupleConverter"
    }
  }

  def converterProviderForImpl[A: c.WeakTypeTag](c: Context): c.Expr[com.criteo.scalaschemas.scalding.parquet.StrictConverterProvider[A]] = {
    import c.universe._

    val tpe = weakTypeOf[A]

    val converters: List[c.universe.Tree] = extractConstructorSymbols[A](c).zipWithIndex.map { case (symbol: Symbol, pos: Int) =>
      val typeSignature = symbol.typeSignature

      val default = if (symbol.asTerm.isParamWithDefault) {
        q"Some(_root_.com.criteo.scalaschemas.SchemaMacroSupport.getDefaultConstructorArgument[${weakTypeOf[A]}, $typeSignature]($pos))"
      } else {
        q"None"
      }

      q"implicitly[_root_.com.criteo.scalaschemas.scalding.parquet.StrictConverterProvider[$typeSignature]].makeFor($default)"
    }

    val typeConstructorArgs: List[c.universe.Tree] = extractConstructorSymbols[A](c).zipWithIndex.map {
      case (symbol: Symbol, pos: Int) => q"converters($pos).currentValue.asInstanceOf[${symbol.typeSignature}]"
    }

    val convertersReset: List[c.universe.Tree] = extractConstructorSymbols[A](c).indices.map { i => q"converters($i).reset()" }.toList

    c.Expr[com.criteo.scalaschemas.scalding.parquet.StrictConverterProvider[A]] {
      q"""
          new _root_.com.criteo.scalaschemas.scalding.parquet.StrictConverterProvider[$tpe] {
            override def makeFor(_default: Option[$tpe]) = new _root_.com.criteo.scalaschemas.scalding.parquet.StrictGroupConverter[$tpe] {
              private val converters = Vector(..$converters)

              override def default: Option[$tpe] = _default

              override def getConverter(fieldIndex: Int) = converters(fieldIndex)

              override def currentValue: $tpe = new $tpe(..$typeConstructorArgs)

              override def reset(): Unit = {
                ..$convertersReset
              }
            }
          }
      """
    }
  }
}
