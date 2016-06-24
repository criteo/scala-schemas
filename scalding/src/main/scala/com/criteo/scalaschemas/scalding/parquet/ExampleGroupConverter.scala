package com.criteo.scalaschemas.scalding.parquet

import parquet.io.api.Converter

/**
  * An example [[parquet.io.api.GroupConverter]] which serves as a (mental) template for what we implement
  * via [[ParquetMacros.converterProviderFor]].
  */
class ExampleGroupConverter extends StrictGroupConverter[Example] {

  private val converters = Array(
    implicitly[StrictConverterProvider[String]].makeFor(None),
    implicitly[StrictConverterProvider[Int]].makeFor(None)
  )

  override def getConverter(fieldIndex: Int): Converter = converters(fieldIndex)

  override def currentValue: Example = new Example(
    name = converters(0).currentValue.asInstanceOf[String],
    value = converters(1).currentValue.asInstanceOf[Int]
  )

  override def reset(): Unit = {
    converters(0).reset()
    converters(1).reset()
  }

  override def default: Option[Example] = None
}

case class Example(name: String, value: Int)