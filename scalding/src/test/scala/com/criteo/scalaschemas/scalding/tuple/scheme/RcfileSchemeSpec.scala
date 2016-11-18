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

class RcfileSchemeSpec extends FunSuite with Matchers with MockitoSugar {
  val CsvFilePath = "scalding/src/test/resources/rc_file_test.csv"
  val PartialCsvFilePath = "scalding/src/test/resources/partial_rc_file_test.csv"
  val TargetRcDirectoryPath = "target/rc_file_test.rc"

  val RcfileColumns = Seq(
    RcfileColumn("intCol", 0, RcfileType.Int),
    RcfileColumn("bigintCol", 1, RcfileType.BigInt),
    RcfileColumn("floatCol", 2, RcfileType.Float),
    RcfileColumn("doubleCol", 3, RcfileType.Double),
    RcfileColumn("booleanCol", 4, RcfileType.Boolean),
    RcfileColumn("stringCol", 5, RcfileType.String),
    RcfileColumn("timestampCol", 6, RcfileType.Timestamp),
    RcfileColumn("smallintCol", 7, RcfileType.SmallInt)
  )

  val ColumnNames: Fields = new Fields(RcfileColumns.map(_.name).toArray
    .asInstanceOf[Array[Comparable[String]]]: _*)

  val ColumnTypes: Seq[RcfileType.EnumVal] = RcfileColumns.map(_.typ)

  test("Write and read back a full RC file") {
    write_and_read_back_an_RC_file()
  }

  test("Write a full RC file and read back only some columns") {
    write_and_read_back_an_RC_file(2, 4)
  }

  /**
    * @param columnsToReadBack empty seq means all
    */
  def write_and_read_back_an_RC_file(columnsToReadBack: Int*) {

    // Write
    {
      val textDelimited: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[AnyRef], Array[AnyRef]] = {
        new TextDelimited(ColumnNames, true, ",")
      }
      val input: Lfs = new Lfs(textDelimited, CsvFilePath)
      deleteQuietly(new File(TargetRcDirectoryPath))
      val output: Lfs = new Lfs(new RcfileScheme(RcfileColumns), TargetRcDirectoryPath)
      val connector: FlowConnector = new HadoopFlowConnector(new Properties)
      val pipe = new Each(new Pipe("write"), new ConvertToHiveJavaType(ColumnNames, ColumnTypes))
      val flow: Flow[_] = connector.connect(input, output, pipe)
      flow.complete()
    }

    // Read back
    {
      val partial = columnsToReadBack.nonEmpty
      val cols = if (!partial) RcfileColumns else RcfileColumns.filter(c => columnsToReadBack.contains(c.index))

      val inputRc: Lfs = new Lfs(new RcfileScheme(cols), TargetRcDirectoryPath)

      val textDelimited: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[AnyRef], Array[AnyRef]] = {
        val colNames = if (!partial) ColumnNames
        else new Fields(cols.map(_.name).toArray.asInstanceOf[Array[Comparable[String]]]: _*)

        new TextDelimited(colNames, true, ",")
      }
      val outputCsv: Lfs = new Lfs(textDelimited, if (partial) PartialCsvFilePath else CsvFilePath)

      val connector: FlowConnector = new HadoopFlowConnector(new Properties)
      val flow: Flow[_] = connector.connect(inputRc, outputCsv, new Pipe("read"))
      val flowProcess = flow.getFlowProcess.asInstanceOf[FlowProcess[JobConf]]
      val rcIt: TupleEntryIterator = inputRc.openForRead(flowProcess)
      val csvIt: TupleEntryIterator = outputCsv.openForRead(flowProcess)
      while (rcIt.hasNext && csvIt.hasNext) {
        val actual: Tuple = rcIt.next.getTuple
        val expected: Tuple = ConvertToHiveJavaType.convert(tuple = csvIt.next.getTuple, types = cols.map(_.typ))
        actual shouldEqual expected
      }
      // Make sure iterators reach their ends.
      assert(!rcIt.hasNext)
      assert(!csvIt.hasNext)
    }
  }
}

private class ConvertToHiveJavaType(fields: Fields, types: Seq[RcfileType.EnumVal])
  extends BaseOperation[Any](types.length, fields)
    with Function[Any] {
  override def operate(flowProcess: FlowProcess[_], functionCall: FunctionCall[Any]): Unit = {
    val input: Tuple = functionCall.getArguments.getTuple

    val output: Tuple = ConvertToHiveJavaType.convert(tuple = input, types = types)
    functionCall.getOutputCollector.add(output)
  }
}

private object ConvertToHiveJavaType {
  def convert(tuple: Tuple, types: Seq[RcfileType.EnumVal]): Tuple = {
    val output: Tuple = new Tuple
    val indexes = types.indices
    for (i <- indexes) {
      val typ = types(i)
      typ match {
        case RcfileType.SmallInt => output.add(tuple.getShort(i))
        case RcfileType.Int => output.add(tuple.getInteger(i))
        case RcfileType.BigInt => output.add(tuple.getLong(i))
        case RcfileType.Float => output.add(tuple.getFloat(i))
        case RcfileType.Double => output.add(tuple.getDouble(i))
        case RcfileType.Boolean => output.add(tuple.getBoolean(i))
        case RcfileType.String => output.add(tuple.getString(i))
        case RcfileType.Timestamp => output.add(Timestamp.valueOf(tuple.getString(i)))
        case _ => throw new IllegalArgumentException(typ + " is not a currently supported Hive type")
      }
    }
    output
  }
}
