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

/**
  * Not Unit tests as we don't want to commit large binary files.
  */
object RcfileUtils {
  /**
    * Not in git: get the file on hdfs
    */
  val RcFilePath = System.getenv("HOME") + "/Documents/hive-example/fc_bc9b417e-a2be-4020-8b18-458209e37ae7"

  def main(args: Array[String]): Unit = {
    read_first_columns_of_a_bi_data_RC_file()
    read_metadata()
  }

  def read_first_columns_of_a_bi_data_RC_file() {
    val columns = Seq(
      RcfileColumn("unix_timestamp", 0, RcfileType.Int),
      RcfileColumn("nb_display", 1, RcfileType.Int),
      RcfileColumn("host_ip", 2, RcfileType.String),
      RcfileColumn("host_site", 3, RcfileType.String),
      RcfileColumn("arbitrage_id", 4, RcfileType.String),
      RcfileColumn("impression_id", 5, RcfileType.String),
      RcfileColumn("display_id", 6, RcfileType.String),
      RcfileColumn("user_id", 7, RcfileType.String),
      RcfileColumn("referrer", 8, RcfileType.String),
      RcfileColumn("user_timestamp", 9, RcfileType.Int),
      RcfileColumn("campaign_id", 10, RcfileType.Int),
      RcfileColumn("banner_id", 11, RcfileType.Int),
      RcfileColumn("merchant_id", 12, RcfileType.Int),
      RcfileColumn("bizmodel_id", 13, RcfileType.Int),
      RcfileColumn("v_revenue_euro", 14, RcfileType.Double),
      RcfileColumn("v_revenue_type", 15, RcfileType.Int),
      RcfileColumn("max_revenue_euro", 16, RcfileType.Double),
      RcfileColumn("max_revenue_type", 17, RcfileType.Int),
      RcfileColumn("display_revenue_euro", 18, RcfileType.Double)
    )
    val columnNames: Fields = new Fields(columns.map(_.name).toArray.asInstanceOf[Array[Comparable[String]]]: _*)

    val textDelimited: Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], Array[AnyRef], Array[AnyRef]] = {
      val sinkCompression = true
      val delimiter = ","
      new TextDelimited(columnNames, sinkCompression, delimiter)
    }

    // Read and display on std
    {
      val inputRc: Lfs = new Lfs(new RcfileScheme(columns), RcFilePath)
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