package com.criteo.scalaschemas.scalding.parquet

import parquet.io.api.{Binary, RecordConsumer}
import parquet.schema.MessageType
import org.joda.time.DateTime

/**
  * A trait that describes how to materialize a given Scala type to a [[RecordConsumer]].
  *
  * @tparam T
  */
trait ParquetWriteRecordSupport[-T] {

  /**
    * The implementer should add the value to the appropriate index in the [[RecordConsumer]].
    *
    * @param value
    * @param rc
    * @param schema
    */
  def writeRecord(value: T, rc: RecordConsumer, schema: MessageType): Unit

}

/**
  * Default implementations.
  */
object ParquetWriteRecordSupport {

  implicit def optionalSupport[A](implicit support: ParquetWriteRecordSupport[A]) =
    new ParquetWriteRecordSupport[Option[A]] {
      override def writeRecord(value: Option[A], rc: RecordConsumer, schema: MessageType): Unit =
        value.foreach(support.writeRecord(_, rc, schema))
    }

  implicit def repeatedSupport[A](implicit support: ParquetWriteRecordSupport[A]) =
    new ParquetWriteRecordSupport[Seq[A]] {
      override def writeRecord(value: Seq[A], rc: RecordConsumer, schema: MessageType): Unit =
        value.foreach(support.writeRecord(_, rc, schema))
    }

  implicit def groupSupport[A](implicit support: (A, RecordConsumer, MessageType) => Unit) =
    new ParquetWriteRecordSupport[A] {
      override def writeRecord(value: A, rc: RecordConsumer, schema: MessageType): Unit = {
        rc.startGroup()
        support(value, rc, schema)
        rc.endGroup()
      }
    }

  implicit val intParquetSchemaSupport = new ParquetWriteRecordSupport[Int] {
    override def writeRecord(value: Int, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addInteger(value)
    }
  }

  implicit val longParquetSchemaSupport = new ParquetWriteRecordSupport[Long] {
    override def writeRecord(value: Long, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addLong(value)
    }
  }

  implicit val shortParquetSchemaSupport = new ParquetWriteRecordSupport[Short] {
    override def writeRecord(value: Short, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addInteger(value)
    }
  }

  implicit val booleanParquetSchemaSupport = new ParquetWriteRecordSupport[Boolean] {
    override def writeRecord(value: Boolean, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addBoolean(value)
    }
  }

  implicit val floatParquetSchemaSupport = new ParquetWriteRecordSupport[Float] {
    override def writeRecord(value: Float, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addFloat(value)
    }
  }

  implicit val doubleParquetSchemaSupport = new ParquetWriteRecordSupport[Double] {
    override def writeRecord(value: Double, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addDouble(value)
    }
  }

  implicit val stringParquetSchemaSupport = new ParquetWriteRecordSupport[String] {
    override def writeRecord(value: String, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addBinary(Binary.fromString(value))
    }
  }

  implicit val jodaTimeSchemaSupport = new ParquetWriteRecordSupport[DateTime] {
    override def writeRecord(value: DateTime, rc: RecordConsumer, schema: MessageType): Unit = {
      rc.addLong(value.getMillis)
    }
  }

}
