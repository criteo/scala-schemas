package com.criteo.scalaschemas.examples

import com.criteo.scalaschemas.hive.HiveCol

class Bar(@HiveCol("varchar(32)") var name: String) {
  def this() = this("foo")
}
