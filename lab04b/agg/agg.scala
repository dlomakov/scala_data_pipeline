// Import Libs
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object agg {
    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.INFO)

        // Установка параметров
        val kafraHost: String = "spark-master-1"
        val kafkaPort: String = "6667"
        val kafkaTopicInput: String = "denis_lomakov"
        val kafkaTopicOutput: String = "denis_lomakov_lab04b_out"
        val offsets: String = "earliest"
        val checkpointLocation: String = "/user/denis.lomakov/tmp/chk/lab04"
        val trigger: String = "5 seconds"
        val mode: String = "update"

        // Spark-сессия
        val spark: SparkSession = SparkSession
          .builder()
          .appName("Lab04_agg")
          .getOrCreate()
        
        // Connect to Kafka
        val kafkaParams = Map(
        "kafka.bootstrap.servers" -> s"$kafraHost:$kafkaPort",
        "subscribe" -> s"$kafkaTopicInput",
        "startingOffsets" -> s"$offsets"
        )

        // читаем данные из топика
        val kafkaMessages = spark.readStream.format("kafka").options(kafkaParams).load()

        // извлекаем value из сообщений kafka
        val kafkaMessagesVal = kafkaMessages.select(col("value").cast("string"))

        // определяем схему данных для извлечения JSON
        val schema = new StructType()
                  .add("event_type", StringType, nullable = true)
                  .add("category", StringType, nullable = true)
                  .add("item_id", StringType, nullable = true)
                  .add("item_price", IntegerType, nullable = true)
                  .add("uid", StringType, nullable = true)
                  .add("timestamp", TimestampType, nullable = true)
        
        // Парсим json
        val eventsDf = kafkaMessagesVal
                        .withColumn("value",from_json(col("value"), schema))
                        .select(col("value.*"))
        
        // считаем агрегаты
        val eventsAggDf = eventsDf.groupBy(window((col("timestamp").cast("bigint") / 1000).cast(TimestampType), "60 minutes"))
                        .agg(
                            sum(when(col("event_type") === "buy", col("item_price"))).alias("revenue"),
                            sum(when(col("uid").isNotNull, lit(1)).otherwise(lit(0))).alias("visitors"),
                            sum(when(col("event_type") === "buy", lit(1)).otherwise(lit(0))).alias("purchases")
                            )
                        .withColumn("aov", col("revenue")/col("purchases"))
                        .withColumn("start_ts", unix_timestamp(col("window.start")))
                        .withColumn("end_ts", unix_timestamp(col("window.end")))
                        .select("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")

        def createKafkaSinc(trigger: String, mode: String, kafkaTopicOutput: String, checkpointLocation: String,  df: DataFrame) = {
            df.selectExpr("to_json(struct(*)) AS value")
            .writeStream
            .format("kafka")
            .trigger(Trigger.ProcessingTime(trigger))
            .outputMode(mode)
            .option("kafka.bootstrap.servers", s"$kafraHost:$kafkaPort")
            .option("topic", kafkaTopicOutput)
            .option("checkpointLocation", checkpointLocation)
            .option("failOnDataLoss",value = false)
        }

        // пишем данные в кафку
        createKafkaSinc(trigger, mode, kafkaTopicOutput, checkpointLocation, eventsAggDf).start.awaitTermination
    }
}