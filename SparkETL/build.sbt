name := "SparkSQLApp"

organization := "com.rh.bi.etl.spark"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"
val jacksonVersion = "2.9.4"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "com.databricks" % "spark-avro_2.11" % "4.0.0",
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" %  jacksonVersion,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.2.jre8",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "com.typesafe" % "config" % "1.3.3"
)
 