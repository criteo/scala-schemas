package com.criteo.scalaschemas.scalding.examples

import com.criteo.scalaschemas.scalding.job.RootArgs
import com.criteo.scalaschemas.scalding.parquet.ParquetMacros
import com.criteo.scalaschemas.scalding.types.ScaldingType
import com.twitter.scalding.JobTest
import org.scalatest.{Matchers, WordSpec}

/**
  * Tests a minimal [[ScaldingType]] with [[ParquetMacros]].
  */
class FooParquetJobSpec extends WordSpec with Matchers {

  "write a Parquet file" in {

    val sourceData = Seq(
      FooParquet("a", BarParquet("b", true), 1),
      FooParquet("b", BarParquet("b", true), 2),
      FooParquet("c", BarParquet("c", true), 3),
      FooParquet("d", BarParquet("z", true), 4),
      FooParquet("d", BarParquet("x", true), 5),
      FooParquet("d", BarParquet("p", true), 6)
    )

    JobTest(new FooParquetJob(_))
      // here we override source with our fake data for the test
      .source[FooParquet](FooParquet.source(RootArgs("/tmp/source")), sourceData)
      // runHadoop actually writes data out, but we can't actually re-read it for inspection :(
      // TODO: fix this since we're not testing much
      .runHadoop.finish

  }
}