
val scalaFullVersion = "2.10.6"
val paradiseVersion = "2.1.0"
val paradiseArtifactName = s"paradise_${scalaFullVersion}"

val scaldingVersion = "0.15.0"
val parquetVersion = "1.6.0rc7"
val hiveExecVersion = "0.10.0"
val jodaTimeVersion = "2.8.1"
val jodaConvertVersion = "1.7"

val scalaTestVersion = "2.2.4"
val mockitoVersion = "1.9.5"

lazy val commonSettings = Seq(
    organization := "com.criteo",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scalaFullVersion,

    libraryDependencies ++= Seq(
      "joda-time" % "joda-time" % jodaTimeVersion,
      "org.joda" % "joda-convert" % jodaConvertVersion,
      compilerPlugin("org.scalamacros" % "paradise" % paradiseVersion cross CrossVersion.full),
      "org.scala-lang" % "scala-reflect" % scalaFullVersion,
      "org.scalamacros" % paradiseArtifactName % paradiseVersion,
      "org.scalamacros" %% "quasiquotes" % paradiseVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.mockito" % "mockito-all" % mockitoVersion % "test"
    )
  )

lazy val root = (project in file(".")).
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
  ).enablePlugins(TutPlugin).dependsOn(core)

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
  ).enablePlugins(TutPlugin).dependsOn(core)

