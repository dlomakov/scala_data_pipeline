// Import Libs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j._

object users_items {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark-сессия
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Lab05_users-items")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()

    // Импорт имплиситов
    import spark.implicits._

    //  Опции из spark.conf
    val updateMode: String = spark.conf.get("spark.users_items.update")
    val inputDir: String = spark.conf.get("spark.users_items.input_dir")
    val outputDir: String = spark.conf.get("spark.users_items.output_dir")

    // Оконка для сортировки по дате, чтобы потом отобрать последние даты
    val WindSpec = Window.partitionBy().orderBy(to_date(col("date"), "yyyyMMdd").desc)

    // Прочитаем информацию о посещениях из json из HDFS
    val view = spark.read.json(inputDir + "/view")
      .where(col("uid").isNotNull)
      .withColumn("max_date", first(to_date(col("date"), "yyyyMMdd")).over(WindSpec))
      .withColumn("new_item_id", concat(lit("view_"), regexp_replace(lower(col("item_id")), "-", "_")))
      .select("uid", "new_item_id", "max_date")

    // Сгруппируем и развернем информацию о посещениях
    val viewMatrix = view
      .groupBy(col("uid"), col("max_date")).pivot("new_item_id")
      .count().na.fill(0)

    // Прочитаем информацию о покупках из json из HDFS
    val buy = spark.read.json(inputDir + "/buy")
      .where(col("uid").isNotNull)
      .withColumn("max_date", first(to_date(col("date"), "yyyyMMdd")).over(WindSpec))
      .withColumn("new_item_id", concat(lit("buy_"), regexp_replace(lower(col("item_id")), "-", "_")))
      .select("uid", "new_item_id", "max_date")

    // Сгруппируем и развернем информацию о покупках
    val buyMatrix = buy.groupBy(col("uid"), col("max_date")).pivot("new_item_id")
      .count().na.fill(0)

    // объединенная просмотров и покупок матрица users * items
    val unionMatrix = viewMatrix.join(buyMatrix, Seq("uid", "max_date"), "full").na.fill(0)

    // определяем максимальную дату в объединенном df
    val maxDate: String = unionMatrix
      .select(col("max_date"))
      .distinct
      .as[String]
      .collect()(0)
      .replace("-","")

    // удаляем колонку с максимальной датой
    val userItemMatrix = unionMatrix.drop(col("max_date"))

    if (updateMode == "1") {
      // читаем старую таблицу и объединяем ее с новым df
      val combineMatrix = spark.read.parquet(outputDir).union(userItemMatrix)

      combineMatrix.write
        .mode("overwrite")
        .parquet(outputDir + "/" + maxDate)
    }
    else {
      userItemMatrix.write
        .mode("append")
        .parquet(outputDir + "/" + maxDate)
    }
  }
}