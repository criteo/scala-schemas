package com.criteo.scalaschemas.examples

import com.criteo.scalaschemas.hive.HiveCol

class FooFoo(@HiveCol("varchar(24)") val name: String, val priority: Int)

