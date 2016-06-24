package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A Hive query property. Used to affect the execution of a
 * [[com.criteo.scalaschemas.hive.queries.HiveQuery HiveQuery]].
 *
 * See [[https://cwiki.apache.org/confluence/display/Hive/Configuration+Properties#ConfigurationProperties-QueryandDDLExecution]].
 *
 * @param name The property name
 * @param value The property value
 */
case class HiveQueryProperty(name: String, value: String) extends HiveQueryFragment {

  /**
   * @return `SET name=value;`
   */
  override def generate: String = s"SET $name=$value;"

  /**
   * @return `--hiveconf name=value`
   */
  def cliArguments: Seq[String] = Seq("--hiveconf", s"$name=$value")

}

/**
 * An ordered list of query properties.
 */
case class HiveQueryProperties(properties: Seq[HiveQueryProperty]) extends HiveQueryFragment {

  /**
   * Adds a query property to the list.
   * @param property The query property to add.
   * @return A new list with the query property added at the end.
   */
  def add(property: HiveQueryProperty): HiveQueryProperties = this.copy(properties = this.properties :+ property)

  /**
   * Adds a query property to the list.
   * @param name The name of the property to add.
   * @param value The value of the property to add.
   * @return A new list with the query property added at the end.
   */
  def add(name: String, value: String): HiveQueryProperties =
    add(HiveQueryProperty(name, value))

  /**
   * @return `SET name1=value1;<br/>SET name2=value2;<br/>...`
   */
  override def generate: String =
    properties.map(_.generate).mkString("\n")

  /**
   * @return `--hiveconf name1=value1 --hiveconf name2=value2 ...`
   */
  def cliArguments: Seq[String] =
    properties.flatMap(_.cliArguments)

  override def toString: String = s"HiveQueryProperties(${properties.mkString(", ")})"

}

object HiveQueryProperties extends HiveQueryProperties(Nil)