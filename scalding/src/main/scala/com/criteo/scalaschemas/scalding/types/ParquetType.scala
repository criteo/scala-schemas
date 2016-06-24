package com.criteo.scalaschemas.scalding.types

import cascading.tuple.{Fields, Tuple, TupleEntry}
import com.twitter.scalding.parquet.tuple.TypedParquet
import com.twitter.scalding.parquet.tuple.scheme.{ParquetReadSupport, ParquetWriteSupport}
import com.twitter.scalding.{Source, TupleConverter, TupleSetter}

import scala.reflect.ClassTag

/**
  *
  * Used in combination with [[com.criteo.scalaschemas.scalding.parquet.ParquetMacros]]
  *
  * @tparam T the type
  * @tparam K the type for the key used in creating partitions
  * @tparam R the read support needed by parquet
  * @tparam W the write support needed by parquet
  */
abstract class ParquetType[T, K, R <: ParquetReadSupport[T] : ClassTag, W <: ParquetWriteSupport[T] : ClassTag]
  extends ScaldingType[T, K] {

  override val fields: Fields = Fields.ALL

  override implicit val converter: TupleConverter[T] = new TupleConverter[T] {
    override def apply(te: TupleEntry): T = te.getObject(0).asInstanceOf[T]

    override def arity: Int = 1
  }

  override implicit val setter: TupleSetter[T] = new TupleSetter[T] {
    override def apply(arg: T): Tuple = {
      val t = Tuple.size(1)
      t.set(0, arg) // new Tuple(arg) doesn't seem to work with scala generic types, but this does.
      t
    }

    override def arity: Int = 1
  }

  override def source(partitionKey: K): Source = TypedParquet[T, R, W](partitions(partitionKey))

  override def sink(partitionKey: K): Source = source(partitionKey)
}
