import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object test {
  def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.ERROR)

        // Spark-сессия
        val spark: SparkSession = SparkSession
          .builder()
          .appName("Lab07_mlproject")
          .config("spark.sql.session.timeZone", "UTC")
          .getOrCreate()

        // Задаем параметры
        spark.conf.set("spark.mlproject.model.path","/user/denis.lomakov/training_model")
        spark.conf.set("spark.mlproject.input_topic_name","denis_lomakov")
        spark.conf.set("spark.mlproject.output_topic_name","denis_lomakov_lab07_out")

        val training_model: String = spark.conf.get("spark.mlproject.model.path")
        val input_topic_name: String = spark.conf.get("spark.mlproject.input_topic_name")
        val output_topic_name: String = spark.conf.get("spark.mlproject.output_topic_name")

        // Параметры Kafka
        val kafraHost: String = "spark-master-1"
        val kafkaPort: String = "6667"
        val checkpoint: String = "/user/denis.lomakov/tmp/chk1"

        // определяем схему данных
        val schema = StructType(
          StructField("uid", StringType, nullable = true) ::
            StructField("visits", ArrayType(
              StructType(
                StructField("timestamp", LongType, nullable = true) ::
                  StructField("url", StringType, nullable = true) :: Nil
              ),
              containsNull = true),
              nullable = true) :: Nil)

        // Загружаем сохраненную обученную модель
        val model = PipelineModel.load(training_model)

        // Connect to Kafka
        val kafkaParams = Map(
          "kafka.bootstrap.servers" -> s"$kafraHost:$kafkaPort",
          "subscribe" -> s"$input_topic_name"
        )

        // читаем данные из топика
        val sdf = spark
          .readStream
          .format("kafka")
          .options(kafkaParams)
          .load()

        // Преобразуем входящие данные в Streaming DF
        val testing = sdf
          .select(from_json(col("value").cast("string"), schema).alias("value"))
          .select(col("value.*"))
          .select(col("uid"), explode(col("visits")).alias("visit"))
          .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
          .withColumn("domain", regexp_replace(col("host"), "www.", ""))
          .dropDuplicates(Seq("uid", "domain"))
          .groupBy(col("uid")).agg(collect_list(col("host")).alias("domains"))
          .select(col("uid"),col("domains"))
      
        // Инференс модели на стриминговых данных
        val dfFinal = model.transform(testing)

        // Метод Kafka-синк
        def createKafkaSink(df: DataFrame) = {
          df.toJSON.writeStream
            .outputMode("update")
            .format("kafka")
            .option("checkpointLocation", checkpoint)
            .option("kafka.bootstrap.servers", s"$kafraHost:$kafkaPort")
            .option("topic", output_topic_name)
            .trigger(Trigger.ProcessingTime("5 seconds"))
        }

        // Создаем Kafka-синк
        val sink = createKafkaSink(dfFinal.select(col("uid"),col("category").alias("gender_age")))
        val sq = sink.start()
        sq.awaitTermination()

  }
}