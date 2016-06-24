package com.criteo.scalaschemas.hive.queries.fragments

/**
 * A concrete Hive partition value.
 *
 * Generally used as part of an [[com.criteo.scalaschemas.hive.queries.HiveOutputQuery HiveOutputQuery]].
 *
 * @param column The name of the partition column.
 * @param value The value for the partition.
 */
case class HivePartitionValue(column: String, value: Option[String]) extends HiveQueryFragment {

  /**
   * @return `column [= 'value']`
   */
  override def generate: String = value match {
    case None => s"$column"
    case Some(v) => s"$column = '$v'"
  }

}

object HivePartitionValue {
  def apply(column: String, value: String): HivePartitionValue = apply(column, Some(value))
}

/**
 * An ordered list of partitions values, defining a complete partition.
 */
case class HivePartitionValues(partitionValues: Seq[HivePartitionValue]) extends HiveQueryFragment {

  /**
   * Adds a partition value to this list.
   * @param partitionValue The partition value to add.
   * @return A new list with the partition value added at the end.
   */
  def add(partitionValue: HivePartitionValue): HivePartitionValues = {
    require(!partitionValues.map(_.column).contains(partitionValue.column),
      s"Duplicated partition name: $partitionValue")
    this.copy(partitionValues = this.partitionValues :+ partitionValue)
  }

  /**
   * Adds a partition value  to this list.
   * @param column The name of the partition column.
   * @param value The optional value of the partition.
   * @return A new list with the partition value added at the end.
   */
  def add(column: String, value: Option[String]): HivePartitionValues =
    add(HivePartitionValue(column, value))

  /**
   * Adds a partition value  to this list.
   * @param column The name of the partition column.
   * @param value The value of the partition.
   * @return A new list with the partition value added at the end.
   */
  def add(column: String, value: String): HivePartitionValues =
    add(HivePartitionValue(column, value))

  /**
   * Checks that all values are defined, i.e. none of them are `None`.
   */
  lazy val allValuesDefined: Boolean = partitionValues.forall(_.value.isDefined)

  /**
   * @return `[PARTITION(column [= 'value'], ...)]`
   */
  override def generate: String = if (partitionValues.nonEmpty)
    s"PARTITION (${partitionValues.map(_.generate).mkString(", ")})"
  else
    ""

  override def toString: String = s"HivePartitionValues(${partitionValues.mkString(", ")})"

  /**
    * Checks that this partition is equivalent to another partition, i.e. has the same list of key and values,
    * regardless of their order.
    * @param other Partition to compare to.
    * @return `true` if they are equivalent, `false` if not.
    */
  def isEquivalentTo(other: HivePartitionValues): Boolean =
    (this.partitionValues.size == other.partitionValues.size) &&
    this.partitionValues.sortBy(_.column) == other.partitionValues.sortBy(_.column)
}

object HivePartitionValues extends HivePartitionValues(Nil) {

  /**
    * @param line The line to build from, from a `SHOW PARTITIONS` query.
    * @return The corresponding [[HivePartitionValues]], or `None` if the line is invalid.
    */
  def fromString(line: String): Option[HivePartitionValues] = {
    // Safe because hive partition keys or values cannot contain a "/" or a "=" (they are encoded
    // and then break everything anyway)
    val partitionValues = line.split("/").map(_.split("="))
    if (partitionValues.isEmpty || partitionValues.exists(_.length != 2)) None
    else Some(new HivePartitionValues(partitionValues.map { value => HivePartitionValue(value(0), value(1))}))
  }

}

/**
 * A full partition definition.
 * @param values The values of the partition.
 * @param location The optional location of the partition.
 */
case class HivePartitionDefinition(values: HivePartitionValues, location: Option[String] = None) extends HiveQueryFragment {
  /**
   * @return `PARTITION(column = 'value', ...)[ LOCATION '...']`
   */
  override def generate: String = location match {
    case Some(loc) => s"${values.generate} LOCATION '$loc'"
    case None => values.generate
  }
}
