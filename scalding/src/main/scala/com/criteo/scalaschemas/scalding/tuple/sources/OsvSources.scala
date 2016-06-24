package com.criteo.scalaschemas.scalding.tuple.sources

import cascading.tap.SinkMode
import cascading.tuple.Fields
import com.criteo.scalaschemas.scalding.types.ScaldingType
import com.twitter.scalding.{DelimitedScheme, FixedPathSource, Source}


trait OsvSources[T, K] {
  self: ScaldingType[T, K] =>

  override def sink(partitionKey: K): Source = MultiplePartitionOsv(partitions(partitionKey))

  override def source(partitionKey: K): Source = MultiplePartitionOsv(partitions(partitionKey), fields)

}

case class MultiplePartitionOsv(p: Seq[String],
                                override val fields: Fields = Fields.ALL,
                                override val sinkMode: SinkMode = SinkMode.REPLACE
                               ) extends FixedPathSource(p: _*) with DelimitedScheme {
  override val separator = "\1"
}