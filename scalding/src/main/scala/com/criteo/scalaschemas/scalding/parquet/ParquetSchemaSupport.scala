package com.criteo.scalaschemas.scalding.parquet

import parquet.schema.PrimitiveType.PrimitiveTypeName
import parquet.schema.{PrimitiveType, GroupType, MessageType, Type}
import parquet.schema.Type.Repetition
import org.joda.time.DateTime

/**
  * A trait that describes how to materialize a given Scala type into a Parquet schema fragment or [[Type]].
  *
  * eg Int -> required INT32, Option[String] -> optional binary, etc.
  *
  * @tparam T
  */
trait ParquetSchemaSupport[-T] {

  def forField(name: String, repetition: Repetition): Type

}

object ParquetSchemaSupport {

  implicit def optionalSupport[A](implicit support: ParquetSchemaSupport[A]) =
    new ParquetSchemaSupport[Option[A]] {
      override def forField(name: String, repetition: Repetition): Type = if (repetition == Repetition.REPEATED)
        support.forField(name, repetition)
      else
        support.forField(name, Repetition.OPTIONAL)

    }

  implicit def repeatedSupport[A](implicit support: ParquetSchemaSupport[A]) =
    new ParquetSchemaSupport[Seq[A]] {
      override def forField(name: String, repetition: Repetition): Type = support.forField(name, Repetition.REPEATED)
    }

  implicit def groupSupport[A](implicit msgType: MessageType) =
    new ParquetSchemaSupport[A] {
      override def forField(name: String, repetition: Repetition): Type = new GroupType(repetition, name, msgType.getFields)
    }

  implicit val intSupport = new ParquetSchemaSupport[Int] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.INT32, name)
  }

  implicit val longSupport = new ParquetSchemaSupport[Long] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.INT64, name)
  }

  implicit val shortSupport = new ParquetSchemaSupport[Short] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.INT32, name)
  }

  implicit val booleanSupport = new ParquetSchemaSupport[Boolean] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.BOOLEAN, name)
  }

  implicit val floatSupport = new ParquetSchemaSupport[Float] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.FLOAT, name)
  }

  implicit val doubleSupport = new ParquetSchemaSupport[Double] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.DOUBLE, name)
  }

  implicit val stringSupport = new ParquetSchemaSupport[String] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.BINARY, name)
  }

  implicit val jodaTimeSupport = new ParquetSchemaSupport[DateTime] {
    override def forField(name: String, repetition: Repetition): Type =
      new PrimitiveType(repetition, PrimitiveTypeName.INT64, name)
  }

}