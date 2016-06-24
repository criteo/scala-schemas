package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A Hive UDF definition.
 *
 * See [[https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-TemporaryFunctions]].
 *
 * @param name Name of the function.
 * @param className Fully qualified class name of the function.
 */
case class HiveUDF(name: String, className: String) extends HiveQueryFragment {
  /**
   * @return `CREATE TEMPORARY FUNCTION name AS 'className';`
   */
  override def generate: String = s"CREATE TEMPORARY FUNCTION $name AS '$className';"
}

/**
 * An ordered list of UDFs.
 */
case class HiveUDFs(val udfs: Seq[HiveUDF]) extends HiveQueryFragment {

  /**
   * Adds an UDF to the list.
   * @param udf The UDF to add.
   * @return A new list with the UDF added at the end.
   */
  def add(udf: HiveUDF): HiveUDFs = this.copy(udfs = this.udfs :+ udf)

  /**
   * Adds an UDF to the list.
   * @param name The name of the UDF to add.
   * @param className The fully qualified class name of the UDF to add.
   * @return A new list with the UDF added at the end.
   */
  def add(name: String, className: String): HiveUDFs =
    add(HiveUDF(name, className))

  /**
   * @return `CREATE TEMPORARY FUNCTION name 'className';...`
   */
  override def generate: String =
    udfs.map(_.generate).mkString("\n")

  override def toString: String = s"HiveUDFs(${udfs.mkString(", ")})"
}

object HiveUDFs extends HiveUDFs(Nil)