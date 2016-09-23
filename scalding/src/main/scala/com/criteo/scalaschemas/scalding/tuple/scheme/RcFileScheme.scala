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

import scala.collection.JavaConverters._

case class SourceContext(key: LongWritable,
                         value: BytesRefArrayWritable)

case class SinkContext(byteStream: ByteStream.Output,
                       rowWritable: BytesRefArrayWritable,
                       colValRefs: Array[BytesRefWritable])

/**
  * Read and write RCFile format in cascading.
  * <p>
  * Warning: Not all field types are supported.
  *
  * @param names             Must contains the ordered list of fields names, can be smaller than the actual file to
  *                          read: remaining columns will be ignored.
  * @param types             If empty all the values will be considered as strings.
  * @param selectedColumnIds If empty all the fields will be returned.
  */
class RcFileScheme(val names: Fields,
                   val types: Seq[String] = Nil,
                   val selectedColumnIds: Seq[Int] = Nil)
  extends Scheme[JobConf,
    RecordReader[LongWritable, BytesRefArrayWritable],
    OutputCollector[Object, Object],
    SourceContext,
    SinkContext](names, names) {

  @transient lazy val columnarSerDe = new ColumnarSerDe

  override def sourceConfInit(flowProcess: FlowProcess[JobConf],
                              tap: Tap[JobConf, RecordReader[LongWritable, BytesRefArrayWritable], OutputCollector[Object, Object]],
                              conf: JobConf): Unit = {
    conf.setInputFormat(classOf[RCFileInputFormat[Object, Object]])
    if (selectedColumnIds.nonEmpty) conf.set(READ_COLUMN_IDS_CONF_STR, selectedColumnIds.mkString(","))
  }

  override def sourcePrepare(flowProcess: FlowProcess[JobConf],
                             sourceCall: SourceCall[SourceContext, RecordReader[LongWritable, BytesRefArrayWritable]]) {
    val properties: Properties = {
      val props: Properties = new Properties

      props.put(LIST_COLUMNS, names.iterator().asScala.mkString(","))

      if (types.nonEmpty) props.put(LIST_COLUMN_TYPES, types.mkString(","))

      props
    }

    columnarSerDe.initialize(flowProcess.getConfigCopy, properties)
    sourceCall.setContext(SourceContext(
      sourceCall.getInput.createKey,
      sourceCall.getInput.createValue
    ))
  }

  override def sourceCleanup(flowProcess: FlowProcess[JobConf],
                             sourceCall: SourceCall[SourceContext, RecordReader[LongWritable, BytesRefArrayWritable]]) {
    sourceCall.setContext(null)
  }

  override def source(flowProcess: FlowProcess[JobConf],
                      sourceCall: SourceCall[SourceContext, RecordReader[LongWritable, BytesRefArrayWritable]]): Boolean = {

    val context = sourceCall.getContext

    if (!sourceCall.getInput.next(context.key, context.value)) return false

    val tuple: Tuple = sourceCall.getIncomingEntry.getTuple
    val value: BytesRefArrayWritable = context.value
    val struct: ColumnarStruct = columnarSerDe.deserialize(value).asInstanceOf[ColumnarStruct]
    val fieldValues = struct.getFieldsAsList.asScala
    tuple.clear()

    // Extract value from lazy types that are not serializable
    def convert(value: Any): Any = {
      value match {
        case string: LazyString => string.getWritableObject.toString
        case integer: LazyInteger => integer.getWritableObject.get
        case long: LazyLong => long.getWritableObject.get
        case float: LazyFloat => float.getWritableObject.get
        case double: LazyDouble => double.getWritableObject.get
        case boolean: LazyBoolean => boolean.getWritableObject.get
        case short: LazyShort => short.getWritableObject.get
        case timestamp: LazyTimestamp => timestamp.getWritableObject.getTimestamp
        case null => null
        case _ => throw new IllegalArgumentException("Unsupported type: " + value.getClass + ".")
      }
    }

    fieldValues.foreach(field => tuple.add(convert(field)))

    true
  }

  override def sinkConfInit(flowProcess: FlowProcess[JobConf],
                            tap: Tap[JobConf, RecordReader[LongWritable, BytesRefArrayWritable], OutputCollector[Object, Object]],
                            conf: JobConf) {
    conf.setOutputKeyClass(classOf[WritableComparable[_]])
    conf.setOutputValueClass(classOf[BytesRefArrayWritable])
    conf.setOutputFormat(classOf[RCFileOutputFormat])
    conf.setInt(COLUMN_NUMBER_CONF_STR, getSinkFields.size)
  }

  override def sinkPrepare(flowProcess: FlowProcess[JobConf],
                           sinkCall: SinkCall[SinkContext, OutputCollector[Object, Object]]) {
    sinkCall.setContext(SinkContext(
      new ByteStream.Output,
      new BytesRefArrayWritable,
      new Array[BytesRefWritable](getSinkFields.size)
    ))
  }

  override def sinkCleanup(flowProcess: FlowProcess[JobConf],
                           sinkCall: SinkCall[SinkContext, OutputCollector[Object, Object]]) {
    sinkCall.setContext(null)
  }

  override def sink(flowProcess: FlowProcess[JobConf],
                    sinkCall: SinkCall[SinkContext, OutputCollector[Object, Object]]) {
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