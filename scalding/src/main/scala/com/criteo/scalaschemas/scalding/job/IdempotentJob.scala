package com.criteo.scalaschemas.scalding.job

import com.twitter.scalding.Tool
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.{GenericOptionsParser, ToolRunner}
import org.slf4j.LoggerFactory


/**
  * Used to implement an Idempotent scalding job that cleans up its target directory before execution.
  *
  * @tparam T the type of the Job being implemented (eg MyScaldingJob).
  */
abstract class IdempotentJob[T] {
  private val logger = LoggerFactory.getLogger(jobClass)

  def jobClass: Class[T]

  /**
    * Create partitions from command line args.  Used to perform the pre-exec cleanup task.
    *
    * @param args
    * @return
    */
  def partitionsToClean(args: Array[String]): Seq[String]

  def main(args: Array[String]): Unit = {
    val conf = new JobConf
    conf.set("mapreduce.job.user.classpath.first", "true")

    val remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    logger.info(s"args remaining post hadoop args clean up: ${remainingArgs.toList}")

    logger.info(s"running clean up...")
    clean(conf, remainingArgs)

    logger.info("running job...")
    ToolRunner.run(conf, new Tool, Array.concat(Array(jobClass.getName), remainingArgs))
  }

  def clean(conf: JobConf, args: Array[String]): Unit = {
    val fs = FileSystem.get(conf)
    partitionsToClean(args).foreach { partition =>
      logger.info(s"recursively cleaning partition: $partition")
      fs.delete(new Path(partition), true)
    }
  }

}
