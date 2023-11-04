import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object features {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Spark-сессия
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab06_features")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    // Задаем параметры
    val inputLogs: String = "/labs/laba03/"
    val inputUserItemsMatrix: String = "/user/denis.lomakov/users-items/20200429"
    val outputDir: String = "/user/denis.lomakov/features"

    // Читаем web-логи из json
    val webLogs: DataFrame = spark.read.json(inputLogs)
      .select(col("uid"), explode(col("visits")))
      .select(col("uid"), col("col.*"))
      .filter(col("uid").isNotNull)

    // отбираем топ 1000 доменов по посещаемости без привязки к пользователю
    val top1000Domains: DataFrame = webLogs
      .withColumn("domain_top",regexp_replace(lower(callUDF("parse_url", col("url"), lit("HOST"))),"www.",""))
      .filter(!(col("domain_top") === ""))
      .groupBy(col("domain_top")).count
      .orderBy(col("count").desc)
      .limit(1000)
      .select("domain_top")
      .orderBy(col("domain_top"))

    // Парсим URL и время в web-логах
    val webLogsDomains: DataFrame = webLogs
      .withColumn("domain",regexp_replace(lower(callUDF("parse_url", col("url"), lit("HOST"))),"www.",""))
      .withColumn("datetime", to_timestamp(from_unixtime(col("timestamp") / 1000)))
      .select("uid", "domain", "datetime")

    // Объединяем распарсенные web-логи и ТОП-100 доменов
    val webLogsDomainsWithTop: DataFrame = webLogsDomains
      .join(top1000Domains, webLogsDomains("domain") === top1000Domains("domain_top"),"outer")
      .select("uid", "domain_top")
      .groupBy("uid").pivot("domain_top").count
      .na.fill(0)
      .coalesce(5)

    // формируем вектор с числами посещений каждого сайта из топ-1000 по посещениям или 0, если не посещал
    val colNames = webLogsDomainsWithTop.drop("uid", "null").schema.fieldNames
    val domainExpr = "array(" + colNames.map (x => "`"+ x +"`").mkString(",") + ") as domain_features"
    val domainFeaturesDf: DataFrame = webLogsDomainsWithTop
      .drop("null")
      .selectExpr("uid", domainExpr)

    // Формируем доп признаки: число посещений по дням недели, число посещений по часам в сутках.
    // долю посещений в рабочие часы (число посещений в рабочие часы/общее число посещений данного клиента)
    // долю посещений в вечерние часы "web_fraction_evening_hours", где рабочие часы - [9, 18), а вечерние - [18, 24).
    val logs_time: DataFrame = webLogsDomains
      .withColumn("datetime", col("datetime").cast(LongType))
      .withColumn("web_day", concat(lit("web_day_"), lower(from_unixtime(col("datetime"),"E"))))
      .withColumn("hour", from_unixtime(col("datetime"),"HH").cast(IntegerType))
      .withColumn("web_hour", concat(lit("web_hour_"), col("hour")))

    val logs_time_2: DataFrame = logs_time
      .groupBy("uid")
      .agg(count("uid").as("count"),
        count(when(col("hour") >= 9 && col("hour") < 18, col("hour"))).as("work_hours"),
        count(when(col("hour") >= 18 && col("hour") < 24, col("hour"))).as("evening_hours"))
      .withColumn("web_fraction_evening_hours", col("evening_hours") / col("count"))
      .withColumn("web_fraction_work_hours", col("work_hours") / col("count"))
      .select(col("uid"), col("web_fraction_work_hours"), col("web_fraction_evening_hours"))
      .coalesce(5)

    val logs_time_hour: DataFrame = logs_time
      .groupBy("uid")
      .pivot("web_hour")
      .count.na.fill(0)
      .coalesce(5)

    val logs_time_day: DataFrame = logs_time
      .groupBy("uid")
      .pivot("web_day")
      .count.na.fill(0)
      .coalesce(5)

    val webLogsDateTimeFeatures: DataFrame = logs_time_2
      .join(logs_time_hour, Seq("uid"), "left")
      .join(logs_time_day, Seq("uid"), "left")
      .coalesce(5)

    // добавляем к матрице ветктор с числами посещений каждого сайта из топ-1000
    val webLogMatrix: DataFrame = webLogsDateTimeFeatures
      .join(domainFeaturesDf, Seq("uid"), "outer")
      .coalesce(5)

    // читаем users-items матрицу (данные о просмотрах и покупках пользователей) из parquet'а
    val userItemMatrix: DataFrame = spark.read.parquet(inputUserItemsMatrix)

    // объединяем матрицу users * items с данными о посещенияю веб сайтов
    val finalMatrix = webLogMatrix
      .join(userItemMatrix, Seq("uid"), "outer")
      .na.fill(0)

    // сохраняем результат в HDFS в parquet
    finalMatrix.coalesce(1).write.mode("overwrite").parquet(outputDir)

  }
}