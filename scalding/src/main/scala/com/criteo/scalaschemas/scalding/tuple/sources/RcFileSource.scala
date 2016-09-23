package com.criteo.scalaschemas.scalding.tuple.sources

import cascading.scheme.Scheme
import cascading.tap.SinkMode
import cascading.tap.SinkMode._
import cascading.tuple.Fields
import com.criteo.scalaschemas.scalding.tuple.scheme.RcFileScheme
import com.criteo.scalaschemas.scalding.types.ScaldingType
import com.twitter.scalding.{FixedPathSource, SchemedSource, Source}
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}

trait RcFileSource[T, K] {
  self: ScaldingType[T, K] =>

  override def sink(partitionKey: K): Source = MultiplePartitionRcFile(partitions(partitionKey))

  override def source(partitionKey: K): Source = MultiplePartitionRcFile(partitions(partitionKey), fields)

}

trait ColumnarSerDeScheme extends SchemedSource

case class MultiplePartitionRcFile(path: Seq[String],
                                   fields: Fields = Fields.ALL,
                                   override val sinkMode: SinkMode = REPLACE)
  extends FixedPathSource(path: _*) with ColumnarSerDeScheme {

  override val hdfsScheme = new RcFileScheme(fields)
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}