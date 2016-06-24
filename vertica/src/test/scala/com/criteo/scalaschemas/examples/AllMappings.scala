package com.criteo.scalaschemas.examples

import com.criteo.scalaschemas.vertica.VerticaCol
import org.joda.time.DateTime


case class AllMappings(someShort: Short,
                       someInt: Int = 88,
                       someLong: Long,
                       someDateTime: DateTime,
                       someFloat: Float,
                       someDouble: Double,
                       someBoolean: Boolean,
                       @VerticaCol("varchar(128)") someString: String)
