package com.criteo.scalaschemas.scalding.tuple.scheme

import java.io.OutputStream
import java.lang.reflect.Type
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.{Scheme, SinkCall, SourceCall}
import cascading.tap.Tap
import cascading.tuple.{Fields, Tuple}
import org.apache.hadoop.hive.ql.io.RCFile.COLUMN_NUMBER_CONF_STR
import org.apache.hadoop.hive.ql.io.{RCFileInputFormat, RCFileOutputFormat}
import org.apache.hadoop.hive.serde.serdeConstants.{LIST_COLUMNS, LIST_COLUMN_TYPES}
import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR
import org.apache.hadoop.hive.serde2.`lazy`._
import org.apache.hadoop.hive.serde2.columnar.{BytesRefArrayWritable, BytesRefWritable, ColumnarSerDe, ColumnarStruct}
import org.apache.hadoop.io.{LongWritable, WritableComparable}
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}

case class SourceContext(key: LongWritable,
                         value: BytesRefArrayWritable)

case class SinkContext(byteStream: ByteStream.Output,
                       rowWritable: BytesRefArrayWritable,
                       colValRefs: Array[BytesRefWritable])

object RcfileType {

  sealed abstract class EnumVal(val name: String) extends Serializable {
    def readObject(): AnyRef = Types.find(_.name == name).get

    override def toString: String = name
  }

  case object String extends EnumVal("string")

  /**
    * A.k.a. Scala Short
    */
  case object SmallInt extends EnumVal("smallint")

  case object Int extends EnumVal("int")

  /**
    * A.k.a. Scala Long
    */
  case object BigInt extends EnumVal("bigint")

  case object Float extends EnumVal("float")

  case object Double extends EnumVal("double")

  case object Boolean extends EnumVal("boolean")

  case object Timestamp extends EnumVal("timestamp")

  val Types = IndexedSeq(String, SmallInt, Int, BigInt, Float, Double, Boolean, Timestamp)
}

case class RcfileColumn(name: String, index: Int, typ: RcfileType.EnumVal)

/**
  * Read and write RCFile format in cascading.
  * <p>
  * Warning: Not all field types are supported.
  *
  * @param columns List of columns to access.
  */
class RcfileScheme(columns: Seq[RcfileColumn])
  extends Scheme[JobConf, RecordReader[LongWritable, BytesRefArrayWritable], OutputCollector[Object, Object],
    SourceContext, SinkContext](new Fields(columns.map(_.name): _*), new Fields(columns.map(_.name): _*)) {

  @transient lazy val columnarSerDe = new ColumnarSerDe

  type TapAlias = Tap[JobConf, RecordReader[LongWritable, BytesRefArrayWritable], OutputCollector[Object, Object]]
  type SourceCallAlias = SourceCall[SourceContext, RecordReader[LongWritable, BytesRefArrayWritable]]
  type SinkCallAlias = SinkCall[SinkContext, OutputCollector[Object, Object]]

  override def sourceConfInit(flowProcess: FlowProcess[JobConf], tap: TapAlias, conf: JobConf): Unit = {
    conf.setInputFormat(classOf[RCFileInputFormat[Object, Object]])
    conf.set(READ_COLUMN_IDS_CONF_STR, columns.map(_.index).mkString(","))
  }

  override def sourcePrepare(flowProcess: FlowProcess[JobConf], sourceCall: SourceCallAlias) {
    val properties = new Properties

    val columnByIndex = columns.groupBy(_.index).mapValues(_.head)
    val names = (0 to columnByIndex.keys.max).map { i =>
      columnByIndex.get(i) match {
        case Some(RcfileColumn(name, _, _)) => name
        case None => s"col$i"
      }
    }
    val types = (0 to columnByIndex.keys.max).map { i =>
      columnByIndex.get(i) match {
        case Some(RcfileColumn(_, _, typ)) => typ
        case None => "string"
      }
    }

    properties.put(LIST_COLUMNS, names.mkString(","))
    properties.put(LIST_COLUMN_TYPES, types.mkString(","))

    columnarSerDe.initialize(flowProcess.getConfigCopy, properties)
    sourceCall.setContext(SourceContext(
      sourceCall.getInput.createKey,
      sourceCall.getInput.createValue
    ))
  }

  override def sourceCleanup(flowProcess: FlowProcess[JobConf], sourceCall: SourceCallAlias) {
    sourceCall.setContext(null)
  }

  override def source(flowProcess: FlowProcess[JobConf], sourceCall: SourceCallAlias): Boolean = {

    val context = sourceCall.getContext

    if (!sourceCall.getInput.next(context.key, context.value)) return false

    val tuple: Tuple = sourceCall.getIncomingEntry.getTuple
    tuple.clear()

    val value: BytesRefArrayWritable = context.value
    val struct: ColumnarStruct = columnarSerDe.deserialize(value).asInstanceOf[ColumnarStruct]

    // Extract value from lazy types that are not serializable
    def convert(value: Any): Any = {
      value match {
        case null => null
        case string: LazyString => string.getWritableObject.toString
        case integer: LazyInteger => integer.getWritableObject.get
        case long: LazyLong => long.getWritableObject.get
        case float: LazyFloat => float.getWritableObject.get
        case double: LazyDouble => double.getWritableObject.get
        case boolean: LazyBoolean => boolean.getWritableObject.get
        case short: LazyShort => short.getWritableObject.get
        case timestamp: LazyTimestamp => timestamp.getWritableObject.getTimestamp
        case _ => throw new IllegalArgumentException("Unsupported type: " + value.getClass + ".")
      }
    }

    columns.foreach { case (RcfileColumn(_, i, _)) => tuple.add(convert(struct.getField(i))) }

    true
  }

  override def sinkConfInit(flowProcess: FlowProcess[JobConf], tap: TapAlias, conf: JobConf) {
    conf.setOutputKeyClass(classOf[WritableComparable[_]])
    conf.setOutputValueClass(classOf[BytesRefArrayWritable])
    conf.setOutputFormat(classOf[RCFileOutputFormat])
    conf.setInt(COLUMN_NUMBER_CONF_STR, getSinkFields.size)
  }

  override def sinkPrepare(flowProcess: FlowProcess[JobConf], sinkCall: SinkCallAlias) {
    sinkCall.setContext(SinkContext(
      new ByteStream.Output,
      new BytesRefArrayWritable,
      new Array[BytesRefWritable](getSinkFields.size)
    ))
  }

  override def sinkCleanup(flowProcess: FlowProcess[JobConf], sinkCall: SinkCallAlias) {
    sinkCall.setContext(null)
  }

  override def sink(flowProcess: FlowProcess[JobConf], sinkCall: SinkCallAlias) {
    val tuple: Tuple = sinkCall.getOutgoingEntry.getTuple
    val context = sinkCall.getContext

    val byteStream: ByteStream.Output = context.byteStream
    val rowWritable: BytesRefArrayWritable = context.rowWritable
    val colValRefs: Array[BytesRefWritable] = context.colValRefs
    if (tuple.size != colValRefs.length)
      throw new IllegalArgumentException("Source tuple size does not match sink size.")

    def sinkField(out: OutputStream, field: Object, fieldType: Type) {
      val fieldString = if (field != null) field.toString else "\\N"
      out.write(fieldString.getBytes)
    }

    byteStream.reset()
    var startPos: Int = 0
    for (i <- colValRefs.indices) {
      colValRefs(i) = new BytesRefWritable
      rowWritable.set(i, colValRefs(i))
      sinkField(byteStream, tuple.getObject(i), tuple.getTypes()(i))
      colValRefs(i).set(byteStream.getData, startPos, byteStream.getCount - startPos)
      startPos = byteStream.getCount
    }

    sinkCall.getOutput.collect(null, rowWritable)
  }

}
