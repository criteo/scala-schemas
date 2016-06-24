package com.criteo.scalaschemas.vertica

import com.criteo.scalaschemas.SchemaUtils

case class VerticaTable(schema: String,
                        name: String,
                        columns: VerticaColumns,
                        dataOrganization: VerticaDataOrganization,
                        tableConstraints: List[VerticaTableConstraint] = List.empty,
                        partitionClause: Option[String] = None) {
  require(columns.columns.nonEmpty, s"Cannot create table $schema.$name with no column")
  dataOrganization.validateAgainstColumns(columns)

  private val partitionStr = partitionClause.map("\nPARTITION BY " + _).getOrElse("")
  private val tableConstraintsStr =
    if (tableConstraints.isEmpty) ""
    else tableConstraints.map(_.make).mkString(",\n", ",\n", "")

  val createQuery =
    s"""CREATE TABLE IF NOT EXISTS $schema.$name (
${columns.tableColumns}$tableConstraintsStr
)
${dataOrganization.make}$partitionStr"""
}

case class VerticaProjection(table: VerticaTable,
                             projectionName: String,
                             columns: VerticaColumns,
                             dataOrganization: VerticaDataOrganization) {
  require(columns.columns.nonEmpty, s"Cannot create projection $projectionName with no column")

  private val projectionColNames = columns.columns.map(_.name)
  private val tableColNames = table.columns.columns.map(_.name)
  private val missingColumns = projectionColNames.filterNot(tableColNames.contains)
  require(missingColumns.isEmpty,
    s"Unknown columns defined in projection $projectionName: ${missingColumns.mkString(", ")}")

  dataOrganization.validateAgainstColumns(columns)

  val createQuery =
    s"""CREATE PROJECTION ${table.schema}.$projectionName
(${columns.projectionColumns})
AS SELECT
${columns.columns.map(_.name).mkString(",")}
FROM ${table.schema}.${table.name}
${dataOrganization.make}"""

}

case class VerticaDataOrganization(orderByColumns: Option[List[String]],
                                   segmentationColumns: Option[List[String]],
                                   kSafety: Int = 1) {
  private val orderByStr = orderByColumns match {
    case None => ""
    case Some(columns) => s"ORDER BY ${columns.mkString(", ")} "
  }

  private val segmentationStr = segmentationColumns match {
    case None => "UNSEGMENTED ALL NODES"
    case Some(columns) => s"SEGMENTED BY HASH(${columns.mkString(", ")}) ALL NODES"
  }

  def validateAgainstColumns(columns: VerticaColumns) = {
    val allCols = columns.columns.map(_.name)
    val colNames = (orderByColumns.getOrElse(List.empty) ++ segmentationColumns.getOrElse(List.empty)).toSet
    val missingColumns = colNames.filterNot(allCols.contains)
    require(missingColumns.isEmpty,
      s"Unknown columns defined in order by or segmented by: ${missingColumns.mkString(", ")}")
  }

  val make = s"$orderByStr$segmentationStr KSAFE $kSafety"
}

case class VerticaColumns(columns: Seq[VerticaColumn]) {
  val duplicates = SchemaUtils.duplicates[VerticaColumn, String](columns)(column => column.name)
  require(duplicates.isEmpty, s"duplicates found in supplied columns: $duplicates")

  def add(column: VerticaColumn): VerticaColumns = this.copy(columns = this.columns :+ column)

  def add(columnName: String,
          dataType: String,
          columnConstraint: Option[VerticaColumnConstraint] = None,
          encoding: Option[String] = None,
          accessRank: Option[Int] = None): VerticaColumns =
    add(VerticaColumn(columnName, dataType, columnConstraint, encoding, accessRank))

  lazy val tableColumns = columns.map(_.tableColumn).mkString(",\n")
  lazy val projectionColumns = columns.map(_.projectionColumn).mkString(",\n")
}

object VerticaColumns extends VerticaColumns(Nil)

case class VerticaColumn(name: String,
                         dataType: String,
                         columnConstraint: Option[VerticaColumnConstraint] = None,
                         encoding: Option[String] = None,
                         accessRank: Option[Int] = None) {
  private lazy val columnConstraintStr = columnConstraint.map(" " + _.make).getOrElse("")
  private lazy val encodingStr = encoding.map(" ENCODING " + _).getOrElse("")
  private lazy val accessRankStr = accessRank.map(" ACCESSRANK " + _).getOrElse("")

  def withEncoding(newEncoding: String) = copy(encoding = Some(newEncoding))

  def withAccessRank(newAccessRank: Int) = copy(accessRank = Some(newAccessRank))

  lazy val tableColumn = s"$name $dataType$columnConstraintStr$encodingStr$accessRankStr"
  lazy val projectionColumn = s"$name$encodingStr$accessRankStr"
}

case class VerticaColumnConstraint(constraint: String, constraintName: Option[String] = None) {
  val make = s"${constraintName.map(name => s"CONSTRAINT $name ").getOrElse("")}$constraint"
}

case class VerticaTableConstraint(constraint: String, constraintName: String) {
  val make = s"CONSTRAINT $constraintName $constraint"
}
