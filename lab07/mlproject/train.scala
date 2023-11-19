import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.Pipeline

object train {
 def main(args: Array[String]): Unit = {

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Spark-сессия
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Lab07_mlproject")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  // Задаем параметры

  val input_train_path: String = spark.conf.get("spark.mlproject.input_train_path")
  val output_train_path: String = spark.conf.get("spark.mlproject.output_train_path")

  // Читаем входной dataframe из json для обучения модели
  val input = spark.read.json(input_train_path)

  // Парсим родимый json, очищаем домены, удаляем дубликаты, группируем домены в лист по пользователю
  val training = input
    .select(col("uid"),col("gender_age"),explode(col("visits")).alias("visit"))
    .withColumn("host", lower(callUDF("parse_url", col("visit.url"), lit("HOST"))))
    .withColumn("domain", regexp_replace(col("host"), "www.", ""))
    .dropDuplicates(Seq("uid", "gender_age", "domain"))
    .groupBy(col("uid"),col("gender_age")).agg(collect_list(col("host")).alias("domains"))
    .select(col("uid"),col("domains"),col("gender_age"))
     
  // Преобразуем список доменов (массив строк) в числовой вектор
  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")
     
  // Индексируем колонку "Пол-возраст" с помощью StringIndexer
  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")
    .fit(training)

  // Параметры логистической регрессии:
  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  // Преобразуем число (номер класса) в gender_age обратно с помощью трансформера IndexToString (обратного к StringIndexer)
  val indexToStringEstimator = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("category")
    .setLabels(indexer.labels)
    
  // Пайплайн модели
  val pipeline = new Pipeline()
    .setStages(Array(cv, indexer, lr, indexToStringEstimator))

  // Обучаем модель пайплайн методом pipeline.fit()
  val model = pipeline.fit(training)

  // Записываем обученный объект PipelineModel в HDFS
  model.write.overwrite().save(output_train_path)

 }
}