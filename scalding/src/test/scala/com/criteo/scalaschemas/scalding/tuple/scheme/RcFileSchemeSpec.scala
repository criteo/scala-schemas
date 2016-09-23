package com.criteo.scalaschemas.scalding.tuple.scheme

import java.io.File
import java.sql.Timestamp
import java.util.Properties

import cascading.flow.hadoop.HadoopFlowConnector
import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.operation.{BaseOperation, Function, FunctionCall}
import cascading.pipe.{Each, Pipe}
import cascading.scheme.Scheme
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.hadoop.Lfs
import cascading.tuple.{Fields, Tuple, TupleEntryIterator}
import org.apache.commons.io.FileUtils.deleteQuietly
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

import scala.collection.immutable.ListMap

class RcFileSchemeSpec extends FunSuite with Matchers with MockitoSugar {
  val CsvFilePath = "scalding/src/test/resources/rc_file_test.csv"
  val TargetRcDirectoryPath = "target/rc_file_test.rc"
  val Columns = ListMap[String, String](
    "intCol" -> "int",
    "bigintCol" -> "bigint", // aka long
    "floatCol" -> "float",
    "doubleCol" -> "double",
    "booleanCol" -> "boolean",
    "stringCol" -> "string",
    "timestampCol" -> "timestamp"
 )
  val ColumnNames: Fields = new Fields(Columns.keys.toArray.asInstanceOf[Array[Comparable[String]]]: _*)
  val ColumnTypes: Array[String] = Columns.values.toArray

  test("Write and read back an RC file") {
    write_and_read_back_an_RC_file()
  }

  test("Write and partially read back an RC file") {
    write_and_read_back_an_RC_file(2, 4)
  }

  /**
    * @param columnsToReadBack empty seq means all
    */
  def write_and_read_back_an_RC_file(columnsToReadBack: Int*) {
    val textDelimited: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[AnyRef], Array[AnyRef]] = {
      val sinkCompression = true
      val delimiter = ","
      new TextDelimited(ColumnNames, sinkCompression, delimiter)
    }

    // Write
    {
      val input: Lfs = new Lfs(textDelimited, CsvFilePath)
      deleteQuietly(new File(TargetRcDirectoryPath))
      val output: Lfs = new Lfs(new RcFileScheme(ColumnNames, ColumnTypes), TargetRcDirectoryPath)
      val connector: FlowConnector = new HadoopFlowConnector(new Properties)
      val pipe = new Each(new Pipe("write"), new ConvertToHiveJavaType(ColumnNames, ColumnTypes))
      val flow: Flow[_] = connector.connect(input, output, pipe)
      flow.complete()
    }

    // Read back
    {
      val inputRc: Lfs = new Lfs(new RcFileScheme(ColumnNames, ColumnTypes, columnsToReadBack), TargetRcDirectoryPath)
      val inputCsv: Lfs = new Lfs(textDelimited, CsvFilePath)
      val connector: FlowConnector = new HadoopFlowConnector(new Properties)
      val flow: Flow[_] = connector.connect(inputRc, inputCsv, new Pipe("read"))
      val flowProcess = flow.getFlowProcess.asInstanceOf[FlowProcess[JobConf]]
      val rcIt: TupleEntryIterator = inputRc.openForRead(flowProcess)
      val csvIt: TupleEntryIterator = inputCsv.openForRead(flowProcess)
      while (rcIt.hasNext && csvIt.hasNext) {
        val actual: Tuple = rcIt.next.getTuple
        val expected: Tuple = ConvertToHiveJavaType.convert(csvIt.next.getTuple, ColumnTypes)
        if (columnsToReadBack.isEmpty)
          actual shouldEqual expected
        else for (i <- 0 until ColumnNames.size()) {
          if (columnsToReadBack.contains(i))
            actual.getObject(i) shouldEqual expected.getObject(i)
          else {
            val value = actual.getObject(i)
            value == null || value == "" shouldBe true // strangely null values can sometime be deserialized as ""
          }
        }
      }
      // Make sure iterators reach their ends.
      assert(!rcIt.hasNext)
      assert(!csvIt.hasNext)
    }
  }
}

private class ConvertToHiveJavaType(fields: Fields, types: Array[String])
  extends BaseOperation[Any](types.length, fields)
    with Function[Any] {
  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]): Unit = {
    val input: Tuple = functionCall.getArguments.getTuple

    val output: Tuple = ConvertToHiveJavaType.convert(input, types)
    functionCall.getOutputCollector.add(output)
  }
}

private object ConvertToHiveJavaType {
  def convert(tuple: Tuple, types: Array[String]): Tuple = {
    val output: Tuple = new Tuple
    for (i <- types.indices) {
      val `type` = types(i).toLowerCase
      `type` match {
        case "int" => output.add(tuple.getInteger(i))
        case "bigint" => output.add(tuple.getLong(i))
        case "float" => output.add(tuple.getFloat(i))
        case "double" => output.add(tuple.getDouble(i))
        case "boolean" => output.add(tuple.getBoolean(i))
        case "binary" => output.add(tuple.getString(i).getBytes)
        case "string" => output.add(tuple.getString(i))
        case "timestamp" => output.add(Timestamp.valueOf(tuple.getString(i)))
        case _ => throw new IllegalArgumentException(`type` + "%s is not a currently supported Hive type")
      }
    }
    output
  }
}
