name := "agg"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
val json4sVersion = "3.2.11"
val sparkStreamingVersion = "2.4.8"
val sparkSqlKafkaVersion = "2.4.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "org.apache.spark" %% "spark-streaming" % sparkStreamingVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkSqlKafkaVersion
)