package com.criteo.scalaschemas.examples

import com.criteo.scalaschemas.hive.HiveCol
import org.joda.time.DateTime


case class AllMappings(someShort: Short,
                       someInt: Int = 88,
                       someLong: Long,
                       someDateTime: DateTime,
                       someFloat: Float,
                       someDouble: Double,
                       someBoolean: Boolean,
                       @HiveCol("varchar(64)") someString: String)
