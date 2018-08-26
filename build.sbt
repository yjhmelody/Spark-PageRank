name := "SparkApp2"

version := "0.3"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
// https://mvnrepository.com/artifact/org.json4s/json4s-native
libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.4"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

logBuffered in Test := false