# Scala Schemas

![Build status](https://api.travis-ci.org/criteo/scala-schemas.svg?branch=master)

Scala Schemas leverages Scala's class definition syntax, which includes the ability to specify defaults, along with Scala's implicit parameter resolution to safely interact with external protocols and systems.

Currently supported systems are:
* Scalding Type Safe API: Parquet and Tuple Sources
* Hive
* Vertica

This project grew out of the need to reduce duplicate/unsafe code in our Hadoop-based ETL/ELT pipelines at Criteo, the core of which for the projects in question was written using the [Type Safe API of Scalding](https://github.com/twitter/scalding/wiki/Type-safe-api-reference).

The Type Safe API of Scalding uses Scala classes as its internal data format and much of the work we encountered in developing our pipelines involved properly deserializing data from various hadoop formats into Scala classes via Scalding.

In addition to the T (Transform) operation we do, however, want to perform an L (Load) operation, which in our case may be to either Hive or Vertica (or both).

Without Scala Schemas this means writing a lot of unsafe type coercion boilerplate code both when getting data "into" Scalding via TypedPipes or "out of" Hadoop via Sqoop, Hive add parition queries or using Vertica bulk load mechanisms.

# DDL/DML vs R/W Support
Hive and Vertica have their own data processing engines with their own internal mechanisms for serializing and deserializing data, but we still need to be able to create DDL statements and properly support type coercion during DML operations.

Scalding, on the other hand, is a pluggable processing framework which combined with its support for arbitrary Scala types means type coercion is coupled to the underlying message format.  Scala Schemas therefore helpfully provides a rich ScaldingType interface that allows us to bind Scala Schemas to Scalding.  You'll find implementations allowing you to easily read and write Cascading Tuple-based formats (think CSV, OSV, TSV, etc.) and [Parquet](https://github.com/apache/parquet-mr).

# An Example Schema
If you're reading this you're probably well aware of how to declare a class in Scala, but here goes anyway:
```scala
case class WebsiteEvent(
  timestamp: DateTime,
  websiteId: Long = -1,
  secure: Boolean,
  path: String,
  queryString: String,
  referrer: Option[URI] // only available if visitor arrives from another domain
)
```
We declare in our contract that websiteId can be empty, and if so we provide a sensible default. Via the use of a Scala Option that referrer can *also* be empty, but no default exists and the processing platform will need to deal with that later.  If any other field is not present then an exception will the thrown during read time.

We support normal classes, too, just make sure your provide accessors:
```scala
class WebsiteEvent(
  val timestamp: DateTime,
  val websiteId: Long = -1,
  val secure: Boolean,
  val path: String,
  val queryString: String,
  val referrer: Option[URI] // only available if visitor arrives from another domain
)
```

# Implementation Details
The core Scala Schemas project doesn't do much on its own (other than provide a consistent way to parse constructor arguments).  The real examples are found in the individual projects:
* [Hive](/hive/src/main/tut/00-Intro.md)
* [Scalding](/scalding/src/main/tut/00-Intro.md)
* [Vertica](/vertica/src/main/tut/00-Intro.md)

# Extending Scala Schemas
The [SchemaMacroSupport](/core/src/main/scala/com/criteo/scalaschemas/SchemaMacroSupport.scala) class provides the macro functions that parse constructor arguments and generates a generic Schema class.  You can look at the [HiveMacros](/hive/src/main/scala/com/criteo/scalaschemas/hive/HiveMacros.scala) implementation for inspiration.
