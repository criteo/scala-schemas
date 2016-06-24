package com.criteo.scalaschemas.scalding.parquet

import com.twitter.scalding.parquet.tuple.scheme.{TupleFieldConverter, PrimitiveFieldConverter, ParquetTupleConverter}
import parquet.io.api.Binary
import org.joda.time.{DateTimeZone, DateTime}


/**
  * Helps us make sure we build the [[StrictConverter]] with the correct default value.  See the companion object
  * for default implementations.
  *
  * @tparam T
  */
trait StrictConverterProvider[T] {
  def makeFor(default: Option[T]): StrictConverter[T]
}

/**
  * Adds better default semantics to [[TupleFieldConverter]]s.
  *
  * @tparam T
  */
trait StrictConverter[T] extends TupleFieldConverter[T] {

  def default: Option[T]

  protected var maybeValue: Option[T] = default

  def asParquetTupleConverter: ParquetTupleConverter[T] = sys.error("this is not a ParquetTupleConverter!")

}

/**
  * For creating [[parquet.io.api.GroupConverter]]s.  You should see [[ParquetMacros.converterProviderFor]]
  * which generates this class.
  *
  * @tparam T
  */
trait StrictGroupConverter[T] extends ParquetTupleConverter[T] with StrictConverter[T] {
  override def asParquetTupleConverter: ParquetTupleConverter[T] = this
}

/**
  * A [[parquet.io.api.PrimitiveConverter]] that requires a default or fails spectacularly for conversions
  * on tuples with missing fields.
  *
  * @param default
  * @tparam T
  */
abstract class StrictPrimitiveConverter[T](override val default: Option[T] = None)
  extends PrimitiveFieldConverter[T] with StrictConverter[T] {

  override def currentValue: T = maybeValue.getOrElse(sys.error("currentValue called on tuple with no value for this field!"))

  override def reset(): Unit = maybeValue = default

}

/**
  * Default implementations of [[StrictConverterProvider]].
  */
object StrictConverterProvider {

  implicit val shortProvider: StrictConverterProvider[Short] = new StrictConverterProvider[Short] {
    override def makeFor(default: Option[Short]) = new StrictPrimitiveConverter[Short](default) {
      override val defaultValue: Short = 0
      
      override def addInt(value: Int): Unit = maybeValue = Some(value.toShort) // should probably throw an exception on overflow
    }
  }

  implicit val intProvider: StrictConverterProvider[Int] = new StrictConverterProvider[Int] {
    override def makeFor(default: Option[Int]) = new StrictPrimitiveConverter[Int](default) {
      override val defaultValue: Int = 0

      override def addInt(value: Int): Unit = maybeValue = Some(value)
    }
  }

  implicit val longProvider: StrictConverterProvider[Long] = new StrictConverterProvider[Long] {
    override def makeFor(default: Option[Long]) = new StrictPrimitiveConverter[Long](default) {
      override val defaultValue: Long = 0

      override def addLong(value: Long): Unit = maybeValue = Some(value)
    }
  }

  implicit val floatProvider: StrictConverterProvider[Float] = new StrictConverterProvider[Float] {
    override def makeFor(default: Option[Float]) = new StrictPrimitiveConverter[Float](default) {
      override val defaultValue: Float = 0

      override def addFloat(value: Float): Unit = maybeValue = Some(value)
    }
  }

  implicit val doubleProvider: StrictConverterProvider[Double] = new StrictConverterProvider[Double] {
    override def makeFor(default: Option[Double]) = new StrictPrimitiveConverter[Double](default) {
      override val defaultValue: Double = 0

      override def addDouble(value: Double): Unit = maybeValue = Some(value)
    }
  }

  implicit val booleanProvider: StrictConverterProvider[Boolean] = new StrictConverterProvider[Boolean] {
    override def makeFor(default: Option[Boolean]) = new StrictPrimitiveConverter[Boolean](default) {
      override val defaultValue: Boolean = false

      override def addBoolean(value: Boolean): Unit = maybeValue = Some(value)
    }
  }

  implicit val stringProvider: StrictConverterProvider[String] = new StrictConverterProvider[String] {
    override def makeFor(default: Option[String]) = new StrictPrimitiveConverter[String](default) {
      override val defaultValue: String = null

      override def addBinary(value: Binary): Unit = maybeValue = Some(value.toStringUsingUTF8)
    }
  }

  implicit val jodaTimeProvider: StrictConverterProvider[DateTime] = new StrictConverterProvider[DateTime] {
    override def makeFor(default: Option[DateTime]) = new StrictPrimitiveConverter[DateTime](default) {
      override val defaultValue: DateTime = new DateTime(0, DateTimeZone.UTC)

      override def addLong(value: Long): Unit = maybeValue = Some(new DateTime(value, DateTimeZone.UTC))
    }
  }
}
