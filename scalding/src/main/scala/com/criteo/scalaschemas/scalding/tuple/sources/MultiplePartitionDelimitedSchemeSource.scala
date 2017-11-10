package com.criteo.scalaschemas.scalding.tuple.sources

import cascading.tap.SinkMode
import cascading.tuple.Fields
import com.twitter.scalding.{DelimitedScheme, FixedPathSource, Source}

case class MultiplePartitionDelimitedSchemeSource(p: Seq[String],
                                                  override val fields: Fields = Fields.ALL,
                                                  override val sinkMode: SinkMode = SinkMode.REPLACE,
                                                  override val strict: Boolean = false,
                                                  override val separator: String = "\t"
                                                 ) extends FixedPathSource(p: _*) with DelimitedScheme {

}

