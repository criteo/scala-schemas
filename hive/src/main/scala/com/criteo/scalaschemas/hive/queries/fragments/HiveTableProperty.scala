package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A Hive table property.
 *
 * @param name The property name
 * @param value The property value
 */
case class HiveTableProperty(name: String, value: String) extends HiveQueryFragment {

  /**
   * @return `'property_name'='property_value'`
   */
  override def generate: String = s"'$name'='$value'"

}

/**
 * An ordered list of table properties.
 */
case class HiveTableProperties(properties: Seq[HiveTableProperty]) extends HiveQueryFragment {

  /**
   * Adds a table property to the list.
   * @param property The table property to add.
   * @return A new list with the table property added at the end.
   */
  def add(property: HiveTableProperty): HiveTableProperties = this.copy(properties = this.properties :+ property)

  /**
   * Adds a table property to the list.
   * @param name The name of the property to add.
   * @param value The value of the property to add.
   * @return A new list with the table property added at the end.
   */
  def add(name: String, value: String): HiveTableProperties =
    add(HiveTableProperty(name, value))

  /**
   * @return `TBLPROPERTIES ('property_name'='property_value', ...)`
   */
  override def generate: String = if (properties.nonEmpty)
    s"TBLPROPERTIES (${properties.map(_.generate).mkString(",\n")})"
  else
    ""

  override def toString: String = s"HiveTableProperties(${properties.mkString(", ")})"
}

object HiveTableProperties extends HiveTableProperties(Nil)