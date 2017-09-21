
val scalaFullVersion = "2.11.11"
val paradiseVersion = "2.1.0"

val scaldingVersion = "0.15.0"
val parquetVersion = "1.6.0rc7"
val hiveExecVersion = "0.10.0"
val jodaTimeVersion = "2.8.1"
val jodaConvertVersion = "1.7"

val scalaTestVersion = "2.2.4"
val mockitoVersion = "1.9.5"

lazy val commonSettings = Seq(
  organization := "com.criteo.scala-schemas",
  version := "0.3.0",
  scalaVersion := scalaFullVersion,

  crossScalaVersions := Seq("2.11.11", "2.10.6"),

  resolvers ++= Seq(
    "conjars.org" at "http://conjars.org/repo",
    "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
  ),

  // Maven config
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    "criteo-oss",
    sys.env.getOrElse("SONATYPE_PASSWORD", "")
  ),
  publishTo := Some(
    if (isSnapshot.value)
      Opts.resolver.sonatypeSnapshots
    else
      Opts.resolver.sonatypeStaging
  ),
  pgpPassphrase := sys.env.get("SONATYPE_PASSWORD").map(_.toArray),
  pgpSecretRing := file(".travis/secring.gpg"),
  pgpPublicRing := file(".travis/pubring.gpg"),
  pomExtra in Global := {
    <url>https://github.com/criteo/cuttle</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git@github.com:criteo/scala-schemas.git</connection>
      <developerConnection>scm:git@github.com:criteo/scala-schemas.git</developerConnection>
      <url>https://github.com/criteo/scala-schemas</url>
    </scm>
    <developers>
      <developer>
        <name>Justin coffey</name>
        <email>j.coffey@criteo.com</email>
        <url>https://github.com/jqcoffey</url>
        <organization>Criteo</organization>
        <organizationUrl>http://www.criteo.com</organizationUrl>
      </developer>
    </developers>
  },

  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {

      // if scala 2.11+ is used, quasiquotes are merged into scala-reflect
      case Some((2, scalaMajor)) if scalaMajor >= 11 => libraryDependencies.value

      // in Scala 2.10, quasiquotes are provided by macro paradise
      case Some((2, 10)) => libraryDependencies.value ++ Seq(
        compilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),
        "org.scalamacros" %% "quasiquotes" % paradiseVersion cross CrossVersion.binary
      )
    }
  },

  libraryDependencies ++= Seq(
    "joda-time" % "joda-time" % jodaTimeVersion,
    "org.joda" % "joda-convert" % jodaConvertVersion,
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
    "org.mockito" % "mockito-all" % mockitoVersion % "test"
  )
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  aggregate(core, hive, vertica, scalding)

lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    name := "scala-schemas-core"
  )

lazy val hive = (project in file("hive")).
  settings(commonSettings: _*).
  settings(
    name := "scala-schemas-hive"
  ).dependsOn(core)

lazy val vertica = (project in file("vertica")).
  settings(commonSettings: _*).
  settings(
    name := "scala-schemas-vertica"
  ).dependsOn(core)

lazy val scalding = (project in file("scalding")).
  settings(commonSettings: _*).
  settings(
    name := "scala-schemas-scalding",

    libraryDependencies ++= Seq(
      "com.twitter" %% "scalding-core" % scaldingVersion,
      "com.twitter" %% "scalding-parquet" % scaldingVersion,
      "com.twitter" % "parquet-column" % parquetVersion,
      "com.twitter" % "parquet-hadoop" % parquetVersion,
      "org.apache.hive" % "hive-exec" % hiveExecVersion exclude("com.google.protobuf", "protobuf-java") exclude("org.apache.avro", "avro-mapred")
    )
  ).dependsOn(core)

