package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A Hive column definition.
 *
 * @param name The name of the column.
 * @param typeName Its hive data type.
 * @param comment An optional comment.
 */
case class HiveColumn(name: String, typeName: String, comment: Option[String] = None) extends HiveQueryFragment {

  /**
   * @return {{{`name` typeName [COMMENT comment]}}}
   */
  override def generate: String = s"`$name` $typeName${if (comment.isDefined) s" COMMENT '${comment.get}'" else ""}"

}

/**
 * An ordered list of Hive columns.
 */
case class HiveColumns(columns: Seq[HiveColumn]) extends HiveQueryFragment {

  /**
   * Adds a column to this list.
   * @param column The column to add.
   * @return A new list with the column added at the end.
   */
  def add(column: HiveColumn): HiveColumns = {
    require(!columns.map(_.name).contains(column.name), s"Duplicated column name: $column")
    this.copy(columns = this.columns :+ column)
  }

  /**
   * Adds a column to this list.
   * @param name The name of the column.
   * @param typeName The type of the column.
   * @param comment An optional comment on the column.
   * @return A new list with the column added at the end.
   */
  def add(name: String, typeName: String, comment: Option[String] = None): HiveColumns =
    add(HiveColumn(name, typeName, comment))

  /**
   * @return {{{(`name` typeName [COMMENT comment], ...)}}}
   */
  override def generate: String = s"(${columns.map(_.generate).mkString(",\n")})"

  override def toString: String = s"HiveColumns(${columns.mkString(", ")})"

}

object HiveColumns extends HiveColumns(Nil)