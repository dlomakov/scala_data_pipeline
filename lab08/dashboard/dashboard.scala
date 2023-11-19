import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.TimestampType

object dashboard {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Общее
    val host = "10.0.0.31"
    val password = "9FX3mXxs"

    // HDFS
    val sourceHdfs = "hdfs:///labs/laba08/laba08.json"
    val modelPath = "/user/denis.lomakov/training_model"

    // ElasticSearch
    val loginElastic = "denis.lomakov"
    val portElastic = "9200"
    val elastcIndex = "denis_lomakov_lab08"

    // Spark-сессия
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab08_dashboard")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    val df = spark.read.json(sourceHdfs)

    val model = PipelineModel.load(modelPath)

    val testing = df
      .select(col("date"),col("uid"), explode(col("visits")).alias("visit"))
      .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .dropDuplicates(Seq("uid", "domain", "date"))
      .groupBy(col("uid"),col("date")).agg(collect_list(col("host")).alias("domains"))
      .select(col("uid"),col("domains"),col("date"))

    val dfFinal = model.transform(testing)

    val esOptions =
      Map(
        "es.nodes" -> host,
        "es.port" -> portElastic,
        "es.batch.write.refresh" -> "false",
        "es.nodes.wan.only" -> "true",
        "es.net.http.auth.user" -> loginElastic,
        "es.net.http.auth.pass" -> password
      )

    dfFinal
      .select(col("uid"), (col("date") / 1000).cast(TimestampType).as("date"),col("category").as("gender_age"))
      .write.mode("append")
      .format("es").options(esOptions)
      .save(elastcIndex + "/_doc")


  }
}