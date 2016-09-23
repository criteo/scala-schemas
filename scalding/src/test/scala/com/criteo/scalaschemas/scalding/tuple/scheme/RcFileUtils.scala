package com.criteo.scalaschemas.scalding.tuple.scheme

import java.net.URI
import java.util.Properties

import cascading.flow.hadoop.HadoopFlowConnector
import cascading.flow.{Flow, FlowConnector, FlowProcess}
import cascading.pipe.Pipe
import cascading.scheme.Scheme
import cascading.scheme.hadoop.TextDelimited
import cascading.tap.hadoop.Lfs
import cascading.tuple.{Fields, TupleEntryIterator}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.RCFile
import org.apache.hadoop.mapred.{JobConf, OutputCollector, RecordReader}

import scala.collection.immutable.ListMap

/**
  * Not Unit tests as we don't want to commit large binary files.
  */
object RcFileUtils {
  /**
    * Not in git: get the file on hdfs
    */
  val RcFilePath = System.getenv("HOME") + "/Documents/hive-example/fc_bc9b417e-a2be-4020-8b18-458209e37ae7"

  def main(args: Array[String]): Unit = {
    read_first_columns_of_a_bi_data_RC_file()
    read_metadata()
  }

  def read_first_columns_of_a_bi_data_RC_file() {
    val columns = ListMap[String, (String, Int)](
      "timestamp" -> ("int", 0),
      "nb_display" -> ("int", 1),
      "host_ip" -> ("string", 2),
      "host_site" -> ("string", 3),
      "arbitrage_id" -> ("string", 4),
      "impression_id" -> ("string", 5),
      "display_id" -> ("string", 6),
      "user_id" -> ("string", 7),
      "referrer" -> ("string", 8), // www.facebook.com
      "user_timestamp" -> ("int", 9), // null
      "campaign_id" -> ("int", 10),
      "banner_id" -> ("int", 11),
      "merchant_id" -> ("int", 12),
      "bizmodel_id" -> ("int", 13), // null
      "v_revenue_euro" -> ("double", 14),
      "v_revenue_type" -> ("int", 15), // null
      "max_revenue_euro" -> ("double", 16),
      "max_revenue_type" -> ("int", 17), // null
      "display_revenue_euro" -> ("double", 18)
    )
    val columnNames: Fields = new Fields(columns.keys.toArray.asInstanceOf[Array[Comparable[String]]]: _*)

    val textDelimited: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[AnyRef], Array[AnyRef]] = {
      val sinkCompression = true
      val delimiter = ","
      new TextDelimited(columnNames, sinkCompression, delimiter)
    }

    // Read and display on std
    {
      val inputRc: Lfs = new Lfs(new RcFileScheme(columnNames), RcFilePath)
      val dummyInput: Lfs = new Lfs(textDelimited, "/dummy/path")
      val connector: FlowConnector = new HadoopFlowConnector(new Properties)
      val flow: Flow[_] = connector.connect(inputRc, dummyInput, new Pipe("read"))
      val flowProcess = flow.getFlowProcess.asInstanceOf[FlowProcess[JobConf]]
      val rcIt: TupleEntryIterator = inputRc.openForRead(flowProcess)

      var lineNumber = 0
      while (rcIt.hasNext) {
        val tuple = rcIt.next.getTuple
        println("" + lineNumber + ": " + tuple)
        lineNumber = lineNumber + 1
      }
    }
  }

  def read_metadata() {
    val path = new Path(RcFilePath)
    val conf = new Configuration
    val fs = FileSystem.get(new URI(path.toString), conf)
    val reader = new RCFile.Reader(fs, path, conf)
    println("Metadata: " + reader.getMetadata)
    println("CompressionCodec: " + reader.getCompressionCodec)
  }

}