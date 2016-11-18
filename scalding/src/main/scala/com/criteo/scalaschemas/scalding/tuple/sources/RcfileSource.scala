package com.criteo.scalaschemas.scalding.tuple.sources

import cascading.scheme.Scheme
import cascading.tap.SinkMode
import cascading.tap.SinkMode._
import com.criteo.scalaschemas.scalding.tuple.scheme.{RcFileScheme, RcfileColumn, RcfileScheme}
import com.criteo.scalaschemas.scalding.types.ScaldingType
import com.twitter.scalding.{FixedPathSource, SchemedSource, Source}
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}

trait RcfileSource[T, K] {
  self: ScaldingType[T, K] =>

  def columns: List[RcfileColumn]

  override def sink(partitionKey: K): Source = MultiplePartitionRcFile(partitions(partitionKey), columns)

  override def source(partitionKey: K): Source = MultiplePartitionRcFile(partitions(partitionKey), columns)

}

trait ColumnarSerDeScheme extends SchemedSource

case class MultiplePartitionRcFile(path: Seq[String],
                                   columns: List[RcfileColumn],
                                   override val sinkMode: SinkMode = REPLACE)
  extends FixedPathSource(path: _*) with ColumnarSerDeScheme {

  override val hdfsScheme = new RcfileScheme(columns)
    .asInstanceOf[Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]]
}
