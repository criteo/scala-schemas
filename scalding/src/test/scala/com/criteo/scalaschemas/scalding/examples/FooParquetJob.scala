package com.criteo.scalaschemas.scalding.examples

import com.criteo.scalaschemas.scalding.job.RootArgs
import com.criteo.scalaschemas.scalding.parquet.ParquetMacros
import com.criteo.scalaschemas.scalding.types.ParquetType
import com.twitter.scalding.parquet.tuple.scheme.{ParquetReadSupport, ParquetTupleConverter, ParquetWriteSupport}
import com.twitter.scalding.{Args, Job}
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType

/**
  * A simple job that reads parquet data from one location and writes to another.
  *
  * Note that at test time, we'll override the source data (that doesn't really exist)
  * with an in-memory sequence.
  *
  * @param args
  */
class FooParquetJob(args: Args) extends Job(args) {
  FooParquet.typedPipe(RootArgs("/tmp/source")).write(FooParquet.typedSink(RootArgs("/tmp/sink")))
}

case class FooParquet(name: String, bar: BarParquet, value: Long)

case class BarParquet(name: String, good: Boolean)

object BarParquet {
  implicit val converterProvider = ParquetMacros.converterProviderFor[BarParquet]
  implicit val writeRecord = ParquetMacros.writeRecordFor[BarParquet]
}

/**
  * The easiest way to work with Parquet is to create a companion object of your type that extends
  * [[ParquetType]].  This brings with it all the needed type converters, sources, sinks, etc. for
  * Scalding.
  */
object FooParquet extends ParquetType[FooParquet, RootArgs, FooParquetReadSupport, FooParquetWriteSupport] {

  // all we have to do is implement our partition finding function.
  override def partitions(key: RootArgs): Seq[String] = s"${key.root}/foo" :: Nil
}

class FooParquetReadSupport extends ParquetReadSupport[FooParquet] {
  // TODO: figure out why this doesn't work when declared in the companion object
  implicit val barSchemaSupport = ParquetMacros.messageTypeFor[BarParquet]

  override val rootSchema: String = ParquetMacros.rootSchemaFor[FooParquet]

  override val tupleConverter: ParquetTupleConverter[FooParquet] =
    ParquetMacros.parquetTupleConverterFor[FooParquet]
}

class FooParquetWriteSupport extends ParquetWriteSupport[FooParquet] {
  // TODO: figure out why this doesn't work when declared in the companion object
  implicit val barSchemaSupport = ParquetMacros.messageTypeFor[BarParquet]

  override val rootSchema: String = ParquetMacros.rootSchemaFor[FooParquet]

  override def writeRecord(r: FooParquet, rc: RecordConsumer, schema: MessageType): Unit =
    ParquetMacros.writeMessageFor[FooParquet](r, rc, schema)
}
