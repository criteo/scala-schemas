package com.criteo.scalaschemas.scalding.tuple

import cascading.tuple.Fields
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}


class TupleMacrosSpec extends WordSpec with Matchers with MockitoSugar {

  "TupleMacros" should {

    "build a typed TupleSetter & TupleConverter" in {
      val tupleSetter = TupleMacros.scaldingTupleSetterFor[AllMappings]

      val tuple = tupleSetter(
        new AllMappings(Short.MaxValue, Int.MaxValue, Long.MaxValue, new DateTime(2000), Float.MaxValue, Double.MaxValue, true, "hiya")
      )
      tuple.getShort(0) shouldEqual Short.MaxValue
      tuple.getInteger(1) shouldEqual Int.MaxValue
      tuple.getLong(2) shouldEqual Long.MaxValue
      tuple.getString(3) shouldEqual "1970-01-01T00:00:02.000Z"
      tuple.getFloat(4) shouldEqual Float.MaxValue
      tuple.getDouble(5) shouldEqual Double.MaxValue
      tuple.getBoolean(6) shouldEqual true
      tuple.getString(7) shouldEqual "hiya"
    }

    "build a typed TupleConverter" in {
      val converter = TupleMacros.scaldingTupleConverterFor[AllMappings]

      import cascading.tuple._

      val tuple = new TupleEntry(
        new Fields("someShort", "someInt", "someLong", "someDateTime", "someFloat", "someDouble", "someBoolean", "someString"),
        new Tuple(
          Short.MaxValue.asInstanceOf[AnyRef],
          Int.MaxValue.asInstanceOf[AnyRef],
          Long.MaxValue.asInstanceOf[AnyRef],
          2000.asInstanceOf[AnyRef],
          Float.MaxValue.asInstanceOf[AnyRef],
          Double.MaxValue.asInstanceOf[AnyRef],
          true.asInstanceOf[AnyRef],
          "hiya".asInstanceOf[AnyRef]
        )
      )

      converter(tuple) shouldEqual AllMappings(
        Short.MaxValue,
        Int.MaxValue,
        Long.MaxValue,
        new DateTime(2000, DateTimeZone.UTC),
        Float.MaxValue,
        Double.MaxValue,
        true,
        "hiya"
      )
    }

    "handle default values in tuples" in {
      val converter = TupleMacros.scaldingTupleConverterFor[AllMappings]

      import cascading.tuple._

      val tuple = new TupleEntry(
        new Fields("someShort", "someInt", "someLong", "someDateTime", "someFloat", "someDouble", "someBoolean", "someString"),
        new Tuple(
          Short.MaxValue.asInstanceOf[AnyRef],
          null,
          Long.MaxValue.asInstanceOf[AnyRef],
          2000.asInstanceOf[AnyRef],
          Float.MaxValue.asInstanceOf[AnyRef],
          Double.MaxValue.asInstanceOf[AnyRef],
          true.asInstanceOf[AnyRef],
          "hiya".asInstanceOf[AnyRef]
        )
      )

      converter(tuple) shouldEqual AllMappings(
        Short.MaxValue,
        88,
        Long.MaxValue,
        new DateTime(2000, DateTimeZone.UTC),
        Float.MaxValue,
        Double.MaxValue,
        true,
        "hiya"
      )
    }

    "build fields in the right order" in {
      val fields = TupleMacros.scaldingFieldsFor[AllMappings]
      fields.equalsFields(
        new Fields("someShort", "someInt", "someLong", "someDateTime",
          "someFloat", "someDouble", "someBoolean", "someString")
      )
    }

    "manage DateTime type serialization" in {
      val parse = TupleReadWrite.dateTimeReadWrite.read _
      val ref = new DateTime("2016-01-01T00:00:00Z").getMillis

      parse("2016-01-01").getMillis shouldEqual ref
      parse("2016-01-01T00:00:00Z").getMillis shouldEqual ref
      parse("2016-01-01 00:00:00.0").getMillis shouldEqual ref
    }
  }

}

case class AllMappings(someShort: Short,
                       someInt: Int = 88,
                       someLong: Long,
                       someDateTime: DateTime,
                       someFloat: Float,
                       someDouble: Double,
                       someBoolean: Boolean,
                       someString: String)
