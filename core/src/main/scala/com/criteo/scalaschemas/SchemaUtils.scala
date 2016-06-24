package com.criteo.scalaschemas

/**
  * Helpful functions for dealing with schemas.
  */
object SchemaUtils {

  /**
    * Finds the duplicates in a [[Seq]]
    *
    * @param elements  the list of elements of type [[A]] to traverse.
    * @param extractFn the function extracting the attributes to compare from an element [[A]]
    * @tparam A
    * @tparam B
    * @return
    */
  def duplicates[A, B](elements: Seq[A])(extractFn: A => B): Seq[B] =
    elements.groupBy(extractFn).map { case (extracted, grouped) =>
      extracted -> grouped.size
    }.filter { case (extracted, count) =>
      count > 1
    }.map { case (extracted, count) =>
      extracted
    }.toSeq

  /**
    * Finds the duplicates in a sequence.
    *
    * @param elements
    * @tparam A
    * @return
    */
  def duplicates[A](elements: Seq[A]): Seq[A] = duplicates[A, A](elements)(a => a)

  /**
    * A class to compact strings, useful for reliable string comparisons
    *
    * @param string
    */
  implicit class StringCompactor(val string: String) extends AnyVal {
    def compact: String = string.replaceAll("\\n+", " ").replaceAll("\\s+", " ").stripPrefix(" ").stripSuffix(" ")
  }

}
