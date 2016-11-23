package com.criteo.scalaschemas.scalding.tuple

import com.criteo.scalaschemas.SchemaMacroSupport
import parquet.io.api.Binary
import scala.util.Try

trait TupleReadWrite[A] {
  def read(value: Any): A
  def safeRead(value: Any, default: () => A): A =
    if (value == null) default()
    else Try(read(value)).getOrElse(default())
  def write(a: A): Any
}

object TupleReadWrite {
  import SchemaMacroSupport.coerce

  implicit def optionReadWrite[A: TupleReadWrite]= new TupleReadWrite[Option[A]] {
    def read(value: Any) = value match {
      case null | "null" | """\\\\N""" => None
      case x => Some(implicitly[TupleReadWrite[A]].read(x))
    }
    override def safeRead(value: Any, default: () => Option[A]) = value match {
      case null | "null" | """\\\\N""" => default()
      case x => Try(Some(implicitly[TupleReadWrite[A]].read(x))).getOrElse(default())
    }
    def write(a: Option[A]) = a.orNull
  }

  implicit val intReadWrite = new TupleReadWrite[Int] {
    def read(value: Any) = coerce(value, classOf[Int]) {
      case x: Number => x.intValue
      case x: String => x.toInt
    }
    def write(a: Int) = a
  }

  implicit val shortReadWrite = new TupleReadWrite[Short] {
    def read(value: Any) = coerce(value, classOf[Short]) {
      case x: Number => x.shortValue
      case x: String => x.toShort
    }
    def write(a: Short) = a
  }

  implicit val longReadWrite = new TupleReadWrite[Long] {
    def read(value: Any) = coerce(value, classOf[Long]) {
      case x: Number => x.longValue
      case x: String => x.toLong
    }
    def write(a: Long) = a
  }

  implicit val booleanReadWrite = new TupleReadWrite[Boolean] {
    def read(value: Any) = coerce(value, classOf[Boolean]) {
      case x: Boolean => x
      case "true" => true
      case "false" => false
    }
    def write(a: Boolean) = a
  }

  implicit val doubleReadWrite = new TupleReadWrite[Double] {
    def read(value: Any) = coerce(value, classOf[Double]) {
      case x: Number => x.doubleValue
      case x: String => x.toDouble
    }
    def write(a: Double) = a
  }

  implicit val floatReadWrite = new TupleReadWrite[Float] {
    def read(value: Any) = coerce(value, classOf[Float]) {
      case x: Number => x.floatValue
      case x: String => x.toFloat
    }
    def write(a: Float) = a
  }

  implicit val stringReadWrite = new TupleReadWrite[String] {
    def read(value: Any) = coerce(value, classOf[String]) {
      case x: Binary => x.toStringUsingUTF8
      case x => x.toString
    }
    def write(a: String) = a
  }

  implicit val dateTimeReadWrite = new TupleReadWrite[org.joda.time.DateTime] {
    import org.joda.time._
    import org.joda.time.format._
    def read(value: Any) = coerce(value, classOf[org.joda.time.DateTime]) {
      case x: Number =>
        new DateTime(x.longValue, DateTimeZone.UTC)
      case x: String =>
        val formats = List(
          ISODateTimeFormat.dateTimeParser,
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS") // used by sqoop imports
        )
        formats.foldLeft(Option.empty[DateTime]) {
          case (None, f) =>
            Try(DateTime.parse(x, f.withZone(DateTimeZone.UTC))).toOption
          case (y, _) =>
            y
        }.getOrElse(sys.error(s"Invalid date format for $x"))
      case x: DateTime =>
        x
    }
    def write(a: DateTime) = a.toDateTime(DateTimeZone.UTC).toString(ISODateTimeFormat.dateTime())
  }

}
