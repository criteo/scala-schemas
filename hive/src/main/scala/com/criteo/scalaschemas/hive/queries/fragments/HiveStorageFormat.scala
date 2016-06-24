package com.criteo.scalaschemas.hive.queries.fragments

/**
 * Marker class for Hive storage formats.
 */
trait HiveStorageFormat extends HiveQueryFragment

/**
 * The Text delimited file format of Hive.
 */
case object TextFileHiveStorageFormat extends HiveStorageFormat {

  override def generate: String = "STORED AS TEXTFILE"

}

/**
 * RCFile format.
 */
case object RCFileHiveStorageFormat extends HiveStorageFormat {

  override def generate: String = "STORED AS RCFILE"

}

/**
 * Parquet File format.
 */
case object ParquetHiveStorageFormat extends HiveStorageFormat {

  override def generate: String =
    """
      |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
      |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    """.stripMargin

}
