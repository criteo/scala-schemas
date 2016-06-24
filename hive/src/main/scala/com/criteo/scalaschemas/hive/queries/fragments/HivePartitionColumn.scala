package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A Hive partition column definition.
 *
 * @param name The name of the column.
 * @param typeName Its hive data type.
 * @param comment An optional comment.
 */
case class HivePartitionColumn(name: String, typeName: String, comment: Option[String] = None)
  extends HiveQueryFragment {

  /**
   * @return `name typeName [COMMENT comment]`
   */
  override def generate: String = s"$name $typeName${if (comment.isDefined) s" COMMENT '${comment.get}'" else ""}"

}

/**
 * An ordered list of partition columnn.
 */
case class HivePartitionColumns(partitionColumns: Seq[HivePartitionColumn]) extends HiveQueryFragment {

  /**
   * Adds a partition column to this list.
   * @param partitionColumn The partition column to add.
   * @return A new list with the partition column added at the end.
   */
  def add(partitionColumn: HivePartitionColumn): HivePartitionColumns = {
    require(!partitionColumns.map(_.name).contains(partitionColumn.name), s"Duplicated partition name: $partitionColumn")
    this.copy(partitionColumns = this.partitionColumns :+ partitionColumn)
  }

  /**
   * Adds a partition column to this list.
   * @param name The name of the partition column.
   * @param typeName The type of the partition column.
   * @param comment An optional comment on the partition.
   * @return A new list with the partition column added at the end.
   */
  def add(name: String, typeName: String, comment: Option[String] = None): HivePartitionColumns =
    add(HivePartitionColumn(name, typeName))


  /**
   * @return `[PARTITIONED BY (name typeName [COMMENT comment], ...)]`
   */
  override def generate: String = if (partitionColumns.nonEmpty)
    s"PARTITIONED BY (${partitionColumns.map(_.generate).mkString(",\n")})"
  else
    ""

  override def toString: String = s"HivePartitionColumns(${partitionColumns.mkString(", ")})"
}

object HivePartitionColumns extends HivePartitionColumns(Nil)