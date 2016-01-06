name := "ScanDataImporter"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.hbase" % "hbase" % "0.98.12.1-hadoop2",
  "org.apache.hbase" % "hbase-client" % "0.98.12.1-hadoop2",
  "org.apache.hbase" % "hbase-common" % "0.98.12.1-hadoop2",
  "org.apache.hadoop" % "hadoop-common" % "2.7.0",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5"

)