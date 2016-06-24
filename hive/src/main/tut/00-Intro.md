# Using Hive with Scala Schemas
Scala Schemas allows you to safely create DDL queries based on your Scala class definition.

An example:
```tut
import org.joda.time.DateTime
import java.net.URI
import com.criteo.scalaschemas.hive._
import com.criteo.scalaschemas.hive.queries._
import com.criteo.scalaschemas.hive.queries.fragments._

case class WebsiteEvent(
  timestamp: DateTime,
  websiteId: Long = -1, // note that Hive doesn't support defaults
  secure: Boolean,
  path: String,
  queryString: String,
  @HiveCol("string") referrer: Option[URI] // nor does it support nullable / not null constraints
)

val hiveSchema = HiveMacros.toHive[WebsiteEvent](
  "events", // database
  "/events", // hdfs root location
  None // partition definitions
)

val createTableHql = HiveCreateTableQuery(hiveSchema)

println(createTableHql.make)
```

which should output:
```
CREATE TABLE IF NOT EXISTS website_event
(`timestamp` string,
`websiteId` bigint,
`secure` boolean,
`path` string,
`queryString` string,
`referrer` string)

STORED AS TEXTFILE
LOCATION '/events/website_event'
;
```

The .toHive function generates a HiveSchema from which you can modify the defaults via its copy() method.
```tut
println(HiveCreateTableQuery(hiveSchema.copy(storageEngine = ParquetHiveStorageFormat)).make)
```

which should then output:
```
CREATE TABLE IF NOT EXISTS website_event
(`timestamp` string,
`websiteId` bigint,
`secure` boolean,
`path` string,
`queryString` string,
`referrer` string)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    
LOCATION '/events/website_event'
;
```

See [com.criteo.scalaschemas.hive.queries](../scala/com/criteo/scalaschemas/hive/queries) for the different type of queries supported.
