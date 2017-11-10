package com.criteo.scalaschemas.scalding.tuple.sources

import cascading.tap.SinkMode
import cascading.tuple.Fields
import com.criteo.scalaschemas.scalding.types.ScaldingType
import com.twitter.scalding.{DelimitedScheme, FixedPathSource, Source}


trait TsvSources[T, K] {
  self: ScaldingType[T, K] =>

  val strict: Boolean = false

  override def sink(partitionKey: K): Source = MultiplePartitionDelimitedSchemeSource(partitions(partitionKey),
    strict = strict)

  override def source(partitionKey: K): Source = MultiplePartitionDelimitedSchemeSource(partitions(partitionKey),
    fields,
    strict = strict)

}