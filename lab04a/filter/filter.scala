// Import Libs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Settings
    val kafraHost: String = "spark-master-1"
    val kafkaPort: String = "6667"

    // Spark-сессия
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab04_filter")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    // Параметры из конфигов спарка
    val kafkaTopic = spark.conf.get("spark.filter.topic_name")
    val offsets = spark.conf.get("spark.filter.offset")
    val OutputHdfsPath = spark.conf.get("spark.filter.output_dir_prefix")

    // Читаем из Kafka
    val kafka_df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$kafraHost:$kafkaPort")
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", if (offsets.contains("earliest")) offsets
      else {
        "{\"" + kafkaTopic + "\":{\"0\":" + offsets + "}}"
      })
      .load

    // Преобразуем сообщения кафки из бинарного формата в строку
    val kafkaValue = kafka_df.select(col("value").cast("string")).toDF

    // определяем схему данных для извлечения JSON
    val schema = new StructType()
      .add("event_type", StringType, nullable = true)
      .add("category", StringType, nullable = true)
      .add("item_id", StringType, nullable = true)
      .add("item_price", IntegerType, nullable = true)
      .add("uid", StringType, nullable = true)
      .add("timestamp", StringType, nullable = true)

    // Парсим json
    val events = kafkaValue.withColumn("value", from_json(col("value"), schema))
      .select(col("value.*"))
      .withColumn("date", regexp_replace(to_date(from_unixtime(col("timestamp") / 1000)).cast("string"), "-", ""))
      .withColumn("p_date", regexp_replace(to_date(from_unixtime(col("timestamp") / 1000)).cast("string"), "-", ""))

    // фильтруем просмотры
    events.filter(col("event_type") === "view")
      .orderBy("date")
      .write.mode("overwrite").partitionBy("p_date").json(OutputHdfsPath + "/view")

    // фильтруем покупки
    events.filter(col("event_type") === "buy")
      .orderBy("date")
      .write.mode("overwrite").partitionBy("p_date").json(OutputHdfsPath + "/buy")
  }
}