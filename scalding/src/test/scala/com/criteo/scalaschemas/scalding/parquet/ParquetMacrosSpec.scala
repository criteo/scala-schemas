package com.criteo.scalaschemas.scalding.parquet

import parquet.io.api._
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, WordSpec}
import org.mockito.Mockito._
import org.mockito.Matchers.any

class ParquetMacrosSpec extends WordSpec with Matchers with MockitoSugar {

  "ParquetMacros" should {

    import ParquetMacros._

    "parse a class into a Parquet schema message" in {
      implicit val complexSchemaSupport = messageTypeFor[ParquetComplexType]
      implicit val complexWriteSupport = writeRecordFor[ParquetComplexType]

      val message = messageTypeFor[ParquetSchemaSupportTestType]

      message.toString shouldEqual
        """message ParquetSchemaSupportTestType {
          |  required int32 someShort;
          |  required int32 someInt;
          |  required int64 someLong;
          |  required int64 someDateTime;
          |  required float someFloat;
          |  required double someDouble;
          |  required boolean someBoolean;
          |  required binary someString;
          |  repeated int64 someList;
          |  optional int32 maybeInt;
          |  required group someComplex {
          |    required int32 value;
          |  }
          |  repeated group manyComplex {
          |    required int32 value;
          |  }
          |  optional group maybeComplex {
          |    required int32 value;
          |  }
          |  repeated group maybeManyComplex {
          |    required int32 value;
          |  }
          |  repeated group manyMaybeComplex {
          |    required int32 value;
          |  }
          |}
          |""".stripMargin
    }

    "create a Parquet writeRecord function" in {
      case class Foo(aString: String, aShort: Short, anInt: Int, aLong: Long,
                     aFloat: Float, aDouble: Double, aBoolean: Boolean,
                     maybeLong: Option[Long], manyInts: Seq[Int])

      implicit val writeSupport = writeRecordFor[Foo]
      val schema = ParquetMacros.messageTypeFor[Foo]
      val mockRecordConsumer = mock[RecordConsumer]

      writeSupport(
        Foo(
          aString = "foo",
          aShort = Short.MaxValue,
          anInt = Int.MaxValue,
          aLong = Long.MaxValue,
          aFloat = Float.MaxValue,
          aDouble = Double.MaxValue,
          aBoolean = true,
          maybeLong = Some(Long.MinValue),
          manyInts = Seq(0, Int.MinValue)
        ),
        mockRecordConsumer,
        schema
      )

      verify(mockRecordConsumer, times(1)).startField("aString", 0)
      verify(mockRecordConsumer, times(1)).addBinary(any(classOf[Binary]))
      verify(mockRecordConsumer, times(1)).endField("aString", 0)

      verify(mockRecordConsumer, times(1)).startField("aShort", 1)
      verify(mockRecordConsumer, times(1)).addInteger(Short.MaxValue)
      verify(mockRecordConsumer, times(1)).endField("aShort", 1)

      verify(mockRecordConsumer, times(1)).startField("anInt", 2)
      verify(mockRecordConsumer, times(1)).addInteger(Int.MaxValue)
      verify(mockRecordConsumer, times(1)).endField("anInt", 2)

      verify(mockRecordConsumer, times(1)).startField("aLong", 3)
      verify(mockRecordConsumer, times(1)).addLong(Long.MaxValue)
      verify(mockRecordConsumer, times(1)).endField("aLong", 3)

      verify(mockRecordConsumer, times(1)).startField("aFloat", 4)
      verify(mockRecordConsumer, times(1)).addFloat(Float.MaxValue)
      verify(mockRecordConsumer, times(1)).endField("aFloat", 4)

      verify(mockRecordConsumer, times(1)).startField("aDouble", 5)
      verify(mockRecordConsumer, times(1)).addDouble(Double.MaxValue)
      verify(mockRecordConsumer, times(1)).endField("aDouble", 5)

      verify(mockRecordConsumer, times(1)).startField("aBoolean", 6)
      verify(mockRecordConsumer, times(1)).addBoolean(true)
      verify(mockRecordConsumer, times(1)).endField("aBoolean", 6)

      verify(mockRecordConsumer, times(1)).startField("maybeLong", 7)
      verify(mockRecordConsumer, times(1)).addLong(Long.MinValue)
      verify(mockRecordConsumer, times(1)).endField("maybeLong", 7)

      verify(mockRecordConsumer, times(1)).startField("manyInts", 8)
      verify(mockRecordConsumer, times(1)).addInteger(0)
      verify(mockRecordConsumer, times(1)).addInteger(Int.MinValue)
      verify(mockRecordConsumer, times(1)).endField("manyInts", 8)

    }

    "create a Parquet writeRecord that writes nothing for None or empty Seqs" in {
      case class Foo(maybeLong: Option[Long], manyInts: Seq[Int])

      implicit val writeSupport = writeRecordFor[Foo]

      val schema = messageTypeFor[Foo]

      val mockRecordConsumer = mock[RecordConsumer]

      writeSupport(
        Foo(
          maybeLong = None,
          manyInts = Seq.empty
        ),
        mockRecordConsumer,
        schema
      )

      verify(mockRecordConsumer, times(1)).startField("maybeLong", 0)
      verify(mockRecordConsumer, never()).addLong(any(classOf[Long]))
      verify(mockRecordConsumer, times(1)).endField("maybeLong", 0)

      verify(mockRecordConsumer, times(1)).startField("manyInts", 1)
      verify(mockRecordConsumer, never()).addInteger(any(classOf[Int]))
      verify(mockRecordConsumer, times(1)).endField("manyInts", 1)
    }

    "create a Parquet writeRecord that supports nested complex types" in {

      implicit val barWriteSupport = writeRecordFor[ABar]
      implicit val barSchemaSupport = messageTypeFor[ABar]

      implicit val schemaSupport = messageTypeFor[AFoo]
      implicit val writeSupport = writeRecordFor[AFoo]

      val mockRecordConsumer = mock[RecordConsumer]

      writeSupport(AFoo(ABar(1)), mockRecordConsumer, schemaSupport)

      verify(mockRecordConsumer, times(1)).startField("aBar", 0)
      verify(mockRecordConsumer, times(1)).startGroup()

      verify(mockRecordConsumer, times(1)).endField("value", 0)
      verify(mockRecordConsumer, times(1)).addInteger(1)
      verify(mockRecordConsumer, times(1)).endField("value", 0)

      verify(mockRecordConsumer, times(1)).endGroup()
      verify(mockRecordConsumer, times(1)).endField("aBar", 0)

    }
  }

  "create StrictConverterProvider that supports all types" in {

    implicit val nestedTypeSupport = ParquetMacros.converterProviderFor[ParquetComplexType]

    val converter = ParquetMacros.converterProviderFor[AllParquetMappings].makeFor(None)

    val rootConverter = converter.asGroupConverter()

    rootConverter.start()
    rootConverter.getConverter(0).asPrimitiveConverter().addInt(Short.MaxValue)
    rootConverter.getConverter(1).asPrimitiveConverter().addInt(Int.MaxValue)
    rootConverter.getConverter(2).asPrimitiveConverter().addLong(Long.MaxValue)
    rootConverter.getConverter(3).asPrimitiveConverter().addLong(1000L)
    rootConverter.getConverter(4).asPrimitiveConverter().addFloat(Float.MaxValue)
    rootConverter.getConverter(5).asPrimitiveConverter().addDouble(Double.MaxValue)
    rootConverter.getConverter(6).asPrimitiveConverter().addBoolean(true)
    rootConverter.getConverter(7).asPrimitiveConverter().addBinary(Binary.fromString("foo"))
    rootConverter.getConverter(8).asGroupConverter().start()
    rootConverter.getConverter(8).asGroupConverter().getConverter(0).asPrimitiveConverter().addInt(Int.MinValue)
    rootConverter.getConverter(8).asGroupConverter().end()
    rootConverter.end()

    val materialized = converter.currentValue

    materialized shouldEqual new AllParquetMappings(
      Short.MaxValue,
      Int.MaxValue,
      Long.MaxValue,
      new DateTime(1000L, DateTimeZone.UTC),
      Float.MaxValue,
      Double.MaxValue,
      true,
      "foo",
      ParquetComplexType(Int.MinValue)
    )

  }

  "ensure we can deserialize multiple records" in {
    case class Root(name: String, child: Child)
    case class Child(name: String)

    implicit val childConverter = ParquetMacros.converterProviderFor[Child]
    val rootConverter = ParquetMacros.converterProviderFor[Root].makeFor(None)

    val groupConverter = rootConverter.asGroupConverter()

    groupConverter.start()
    groupConverter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("foo"))
    groupConverter.getConverter(1).asGroupConverter().start()
    groupConverter.getConverter(1).asGroupConverter().getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("bar"))
    groupConverter.getConverter(1).asGroupConverter().end()
    groupConverter.end()

    rootConverter.currentValue shouldEqual Root("foo", Child("bar"))

    groupConverter.start()
    groupConverter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("bim"))
    groupConverter.getConverter(1).asGroupConverter().start()
    groupConverter.getConverter(1).asGroupConverter().getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("bam"))
    groupConverter.getConverter(1).asGroupConverter().end()
    groupConverter.end()

    rootConverter.currentValue shouldEqual Root("bim", Child("bam"))

  }

  "create Parquet converter that supports default values" in {
    val converter = ParquetMacros.converterProviderFor[ParquetMappingsWithDefaults].makeFor(None)

    val rootConverter = converter.asGroupConverter()

    rootConverter.start()
    rootConverter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("foo"))
    rootConverter.end()

    converter.currentValue shouldEqual new ParquetMappingsWithDefaults("foo")

  }

}

case class ABar(value: Int)

case class AFoo(aBar: ABar)

class ParquetSchemaSupportTestType(val someShort: Short,
                                   val someInt: Int = 88,
                                   val someLong: Long,
                                   val someDateTime: DateTime,
                                   val someFloat: Float,
                                   val someDouble: Double,
                                   val someBoolean: Boolean,
                                   val someString: String,
                                   val someList: List[Long],
                                   val maybeInt: Option[Int],
                                   val someComplex: ParquetComplexType,
                                   val manyComplex: List[ParquetComplexType],
                                   val maybeComplex: Option[ParquetComplexType],
                                   val maybeManyComplex: Option[List[ParquetComplexType]],
                                   val manyMaybeComplex: List[Option[ParquetComplexType]])

case class ParquetComplexType(value: Int)

object ParquetComplexType {
  implicit val schemaSupport = ParquetMacros.messageTypeFor[ParquetComplexType]
  implicit val writeSupport = ParquetMacros.writeRecordFor[ParquetComplexType]
  implicit val converterSupport = ParquetMacros.converterProviderFor[ParquetComplexType]
}

case class AllParquetMappings(someShort: Short,
                              someInt: Int,
                              someLong: Long,
                              someDateTime: DateTime,
                              someFloat: Float,
                              someDouble: Double,
                              someBoolean: Boolean,
                              someString: String,
                              someComplex: ParquetComplexType)

case class ParquetMappingsWithDefaults(someString: String, someInt: Int = 123)
