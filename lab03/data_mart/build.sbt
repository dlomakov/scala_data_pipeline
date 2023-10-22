name := "data_mart"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
val cassandraVersion = "2.4.3"
val elasticsearchVersion = "6.8.2"
val postgresqlVersion = "42.3.3"
val json4sVersion = "3.2.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraVersion,
  "org.elasticsearch" %% "elasticsearch-spark-20" % elasticsearchVersion,
  "org.postgresql" % "postgresql" % postgresqlVersion
)