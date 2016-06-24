package com.criteo.scalaschemas.scalding.types

import cascading.flow.FlowDef
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.typed.{TypedPipe, TypedSink}

/**
  * Formalizes the creation of a type for use in scalding jobs.
  *
  * @tparam T the type
  * @tparam K the type for the key used in creating partitions
  */
trait ScaldingType[T, K] {

  implicit def converter: TupleConverter[T]

  implicit def setter: TupleSetter[T]

  def fields: Fields

  def partitions(key: K): Seq[String]

  def source(partitionKey: K): Source

  def sink(partitionKey: K): Source

  def typedPipe(partitionKey: K)(implicit flowDef: FlowDef, mode: Mode): TypedPipe[T] =
    TypedPipe.from(source(partitionKey).read, fields)

  def typedSink(partitionKey: K): TypedSink[T] = TypedSink(sink(partitionKey))

}