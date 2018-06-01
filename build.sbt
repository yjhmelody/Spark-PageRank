name := "SparkApp2"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/io.netty/netty-all
//libraryDependencies += "io.netty" % "netty-all" % "4.1.23.Final"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.5" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase
libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.6" pomOnly()
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-http
libraryDependencies += "com.typesafe.akka" %% "akka-http" % "10.1.1"
// https://mvnrepository.com/artifact/org.json4s/json4s-native
libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.4"

