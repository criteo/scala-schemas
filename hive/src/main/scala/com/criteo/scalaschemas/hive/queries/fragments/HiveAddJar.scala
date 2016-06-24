package com.criteo.scalaschemas.hive.queries.fragments

/**
 * The definition of a jar to add.
 *
 * See [[https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Cli#LanguageManualCli-HiveResources]].
 *
 * @param location Location where the jar file can be found.
 */
case class HiveAddJar(location: String) extends HiveQueryFragment {
  /**
   * @return `ADD JAR 'location';`
   */
  override def generate: String = s"ADD JAR '$location';"
}

/**
 * An ordered list of jars to add.
 */
case class HiveAddJars(jars: Seq[HiveAddJar]) extends HiveQueryFragment {

  /**
   * Adds a jar to the list.
   * @param jar The jar to add.
   * @return A new list with the jar added at the end.
   */
  def add(jar: HiveAddJar): HiveAddJars = this.copy(jars = this.jars :+ jar)

  /**
   * Adds a jar to the list.
   * @param location Location where the jar file can be found.
   * @return A new list with the jar added at the end.
   */
  def add(location: String): HiveAddJars = add(HiveAddJar(location))

  /**
   * @return `ADD JAR 'location';...`
   */
  override def generate: String =
    jars.map(_.generate).mkString("\n")

  override def toString: String = s"HiveAddJars(${jars.mkString(", ")})"
}

object HiveAddJars extends HiveAddJars(Nil)