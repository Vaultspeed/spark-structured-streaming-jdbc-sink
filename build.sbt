val spark_version = "3.1.1"

name := "spark-structured-streaming-jdbc-sink"
version := spark_version
scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % spark_version