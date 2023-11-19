name := "dashboard"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
val elasticsearchVersion = "6.8.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark-20" % elasticsearchVersion
)