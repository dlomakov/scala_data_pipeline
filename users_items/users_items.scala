// Import Libs
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
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
      .withColumn("item_id", concat(lit("view_"), regexp_replace(lower(col("item_id")), "-", "_")))
      .select("uid", "item_id", "max_date")

    // Прочитаем информацию о покупках из json из HDFS
    val buy: DataFrame = spark.read.json(inputDir + "/buy")
      .where(col("uid").isNotNull)
      .withColumn("max_date", first(to_date(col("date"), "yyyyMMdd")).over(WindSpec))
      .withColumn("item_id", concat(lit("buy_"), regexp_replace(lower(col("item_id")), "-", "_")))
      .select("uid", "item_id", "max_date")

    // объединение просмотров и покупок в один DF
    val unionVisits: DataFrame = view.unionAll(buy)

    // определяем максимальную дату в объединенном df
    val maxDate: String = unionVisits
      .select(col("max_date"))
      .distinct
      .as[String]
      .collect()(0)
      .replace("-","")

    // удаляем колонку с максимальной датой
    val dfVisits = unionVisits.drop(col("max_date"))

    // Метод для чтения записанной матрицы и ее unpivot'а
    def read_and_unpivot(): DataFrame = {
      val oldUserItemMatrix = spark.read.parquet(outputDir + "/20200429")
      val cols = oldUserItemMatrix.columns.filter(_.toLowerCase != "uid")
      val alias_cols = "item_id,value"
      val stack_exp = cols
        .map(x => s"""'${x}',${x}""")
        .mkString(s"stack(${cols.length},", ",", s""") as (${alias_cols})""")

      val unpivotOldMatrix = oldUserItemMatrix
        .select(col("uid"),expr(s"""${stack_exp}"""))
        .filter(col("value")==="1")
        .drop(col("value"))

      unpivotOldMatrix
    }

    // Сгруппируем и развернем информацию в матрицу
    def pivotnutyi_df(df: DataFrame): DataFrame = {
      val unionMatrix = df
        .groupBy(col("uid"))
        .pivot("item_id")
        .count().na.fill(0)
      unionMatrix
    }


    if (updateMode == "1") {
      // читаем старую таблицу и объединяем ее с новым df
      val oldUserItemMatrix = read_and_unpivot()

      val newMatrix: DataFrame = oldUserItemMatrix.unionAll(dfVisits)
      val finalMatrix = pivotnutyi_df(newMatrix).coalesce(1)

      finalMatrix.write
        .mode("overwrite")
        .parquet(outputDir + "/" + maxDate)
    }
    else {
      val finalMatrix = pivotnutyi_df(dfVisits).coalesce(1)
      finalMatrix.write
        .mode("append")
        .parquet(outputDir + "/" + maxDate)
    }
  }
}