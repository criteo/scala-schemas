# scala-schemas scalding
The Scalding implementation is designed to work with the [Scalding Type Safe API](https://github.com/twitter/scalding/wiki/Type-safe-api-reference).  There are Tuple-based and Parquet implementations.

# A Schema
```tut
import java.net.URI
import org.joda.time.DateTime

case class WebsiteEvent(
  timestamp: DateTime,
  websiteId: Long = -1,
  secure: Boolean,
  path: String,
  queryString: String,
  referrer: Option[URI] // only available if visitor arrives from another domain
)
```

# Tuple based
Back in the olden days there were Cascading Tuples and things were great.  Think of a Tuple as something like a JDBC ResultSet.  In our case, the framework gives you a Tuple and you need to tell it how to translate this to your class instance.  Easy, right?

I know I sure do love to write lots of code like:
```scala
... show some crappy getInt and setObject code here ...
```

If you're working with case classes exclusively and don't care about defaults and the like, then Scalding already provides pretty much everything you need to get going.  If you're here, though, it's probably because you hit a constraint somewhere that you needed to get around.

## ScaldingType[T, K]
The [ScaldingType[T, K]](scalding/src/main/scala/com/criteo/scalaschemas/scalding/types/ScaldingType.scala) trait centralizes the support classes needed to make Scalding work under "any" circumstances.  By "any", I of course mean your particular circumstances that may not fit in the off-the-shelf Scalding tool kit.

The standard pattern is to implement ScaldingType as a mixin on the companion object of your schema class:
```
import com.criteo.scalaschemas.scalding.types.ScaldingType

case class PartitionKey(root: String)

object WebsiteEvent extends ScaldingType[WebsiteEvent, PartitionKey] {
  override implicit def converter: TupleConverter[WebsiteEvent] = ???
  
  override implicit def setter: TupleSetter[WebsiteEvent] = ???

  override def fields: Fields = ???

  override def partitions(key: PartitionKey): Seq[String] = ???

  override def source(partitionKey: PartitionKey): Source = ???

  override def sink(partitionKey: PartitionKey): Source = ???
}
```
The implicit declaration for the setter and converter just means that whenever you use this class in a Scalding workflow, Scalding will have access to your specific implementations and not use its defaults.

If it looks like you need to implement a lot of code at this point, don't worry.  Macros take care of the details!

### TupleSetter[T] and TupleConverter[T]
Generating Tuple setters and converters is easy as pie:
```tut
import com.criteo.scalaschemas.scalding.tuple._

// we have a URI object that we don't know how to read and write, so let's do that here
implicit val uriTupleReadWrite = new TupleReadWrite[URI] {
  override def read(value: Any): URI = new URI(value.toString)
  override def write(a: URI): Any = a.toString
}

val setter = TupleMacros.scaldingTupleSetterFor[WebsiteEvent]
val converter = TupleMacros.scaldingTupleConverterFor[WebsiteEvent]
```

### TupleReadWrite[T]
So that we can support reading and writing arbitrary Scala types (like URIs) we have provided the [TupleReadWrite[T]](../scala/com/criteo/scalaschemas/scalding/tuple/TupleReadWrite.scala) trait and you see above an implementation for working with URIs.

```scala
trait TupleReadWrite[A] {
  def read(value: Any): A
  def write(a: A): Any
}
```
By declaring that as an implicit value in scope, Scala will make use of it any time the type is needed.  You can read more about implict resolution in Scala here: http://docs.scala-lang.org/tutorials/FAQ/finding-implicits.html.

We have provided default implementations for common types in the [TupleReadWrite](../scala/com/criteo/scalaschemas/scalding/tuple/TupleReadWrite.scala) companion object, but you can override them by declaring a new implementation and importing them into your local scope.

### Fields
We can also generate fields:
```tut
val fields = TupleMacros.scaldingFieldsFor[WebsiteEvent]
```
### Partitions
We don't provide any partition functions, so you'll have to roll your own, but you make use of the partition key you've declared as the K type parameter:
```tut:silent
case class PartitionKey(root: String)
```
```tut
def partitions(key: PartitionKey): Seq[String] = Seq(s"${key.root}/website_event")
```
### Sources and Sinks
These are dependent on the underlying data format and we provide mixinable traits for OSV and TSV sources, [OsvSources](scalding/src/main/scala/com/criteo/scalaschemas/scalding/tuple/sources/OsvSources.scala) and [TsvSources](scalding/src/main/scala/com/criteo/scalaschemas/scalding/tuple/sources/TsvSources.scala) respectively.
### Bringing it all together
```tut
import com.criteo.scalaschemas.scalding.tuple._
import com.criteo.scalaschemas.scalding.tuple.sources._
import com.criteo.scalaschemas.scalding.types.ScaldingType
import cascading.tuple.Fields
import com.twitter.scalding._
import com.twitter.scalding.typed.{TypedPipe, TypedSink}

case class PartitionKey(root: String)

object WebsiteEvent extends ScaldingType[WebsiteEvent, PartitionKey] with TsvSources[WebsiteEvent, PartitionKey] {
  
  implicit val uriTupleReadWrite = new TupleReadWrite[URI] {
    override def read(value: Any): URI = new URI(value.toString)
    override def write(a: URI): Any = a.toString
  }
  
  override implicit val converter: TupleConverter[WebsiteEvent] =
    TupleMacros.scaldingTupleConverterFor[WebsiteEvent]
  
  override implicit val setter: TupleSetter[WebsiteEvent] =
    TupleMacros.scaldingTupleSetterFor[WebsiteEvent]

  override val fields: Fields = TupleMacros.scaldingFieldsFor[WebsiteEvent]

  override def partitions(key: PartitionKey): Seq[String] =
    Seq(s"${key.root}/website_event")
}
```
### Using ScaldingType[T, K] in a Scalding Job
While a complete run through of the scalding API is out of scope for this documentation, the TL;DR looks something like the following.

First, let's create a second class which we'll use as the schema of the output of the job we're going to write:
```tut
// a class that we'll use to aggregate secure/unsecure site visits
case class WebsiteEventSecureStats(secure: Boolean, events: Long)

// implement ScaldingType for this class
object WebsiteEventSecureStats extends ScaldingType[WebsiteEventSecureStats, PartitionKey] with TsvSources[WebsiteEventSecureStats, PartitionKey] {

  override implicit val converter: TupleConverter[WebsiteEventSecureStats] =
    TupleMacros.scaldingTupleConverterFor[WebsiteEventSecureStats]
  
  override implicit val setter: TupleSetter[WebsiteEventSecureStats] =
    TupleMacros.scaldingTupleSetterFor[WebsiteEventSecureStats]

  override val fields: Fields = TupleMacros.scaldingFieldsFor[WebsiteEventSecureStats]

  override def partitions(key: PartitionKey): Seq[String] =
    Seq(s"${key.root}/website_event_secure_stats")
}
```
And then we declare the actual job (which isn't doing any aggregation and is completely useless, but you get the idea:
```tut
import com.twitter.scalding.{Job, Args}

class WebsiteEventJob(args: Args) extends Job(args) {
  val partitionKey = PartitionKey(args("root"))

  WebsiteEvent.typedPipe(partitionKey).map { event =>
      new WebsiteEventSecureStats(secure = event.secure, events = 1)
    }.write(WebsiteEventSecureStats.typedSink(partitionKey))
}
```
# Parquet
We also support reading and writing from and to Parquet files.  We bypass flattening individual values into Tuples and instead write the entire Parquet record into a single Tuple field.

Parquet also has its own set of primitives and we need to know how to convert back and forth between them.  We've also tried to reuse as much code from Scalding's TypedParquet implementation as possible.

This all means it's slightly more complicated to use Parquet than Tuple-based sources.

## ParquetWriteSupport[T] and ParquetReadSupport[T]
These are the classes that we need to convert to and from a Parquet message.  They're abstract classes defined by the core Scalding Parquet API and to implement them we need to implement:
* The creation of a Parquet schema definition
* A ParquetTupleConverter (recall that while we don't flatten values into a Tuple, Scalding imposes it as transport and we read the entire message out of a single bucket)
* A function to write our type to a Parquet RecordConsumer.

Of course, as before this is all done via Scala Macros, so it's just a bunch of delegation:
```tut
import com.criteo.scalaschemas.scalding.parquet.ParquetMacros
import com.twitter.scalding.parquet.tuple.scheme.{ParquetReadSupport, ParquetTupleConverter, ParquetWriteSupport}
import parquet.io.api.RecordConsumer
import parquet.schema.MessageType

class WebsiteEventParquetReadSupport extends ParquetReadSupport[WebsiteEvent] {
  implicit val uriSchemaSupport = ParquetMacros.messageTypeFor[URI]
  implicit val uriConverterProvider = ParquetMacros.converterProviderFor[URI]
  
  override val rootSchema: String = ParquetMacros.rootSchemaFor[WebsiteEvent]

  override val tupleConverter: ParquetTupleConverter[WebsiteEvent] =
    ParquetMacros.parquetTupleConverterFor[WebsiteEvent]
}

class WebsiteEventParquetWriteSupport extends ParquetWriteSupport[WebsiteEvent] {
  override val rootSchema: String = ParquetMacros.rootSchemaFor[WebsiteEvent]

  override def writeRecord(r: WebsiteEvent, rc: RecordConsumer, schema: MessageType): Unit =
    ParquetMacros.writeMessageFor[WebsiteEvent](r, rc, schema)
}
```
With this we can now redefine our companion object using Parquet sources and sinks.
```tut
import com.criteo.scalaschemas.scalding.types.ParquetType

object WebsiteEvent extends ParquetType[WebsiteEvent, PartitionKey, WebsiteEventParquetReadSupport, WebsiteEventParquetWriteSupport] {

  // all we have to do is implement our partition finding function.
  override def partitions(key: PartitionKey): Seq[String] = Seq(s"${key.root}/website_event")
}
```


