import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import java.net.URLDecoder
import scala.util.{Try, Success, Failure}
import java.sql.DriverManager

object data_mart {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Общее
    val host = "10.0.0.31"
    val password = "9FX3mXxs"

    // Cassandra
    val portCassandra = "9042"
    val sourceCassandraKeyspace = "labdata"
    val sourceCassandraClients = "clients"

    // PostgreSQL
    val urlPostgre = "jdbc:postgresql://10.0.0.31:5432"
    val loginPostgre = "denis_lomakov"
    val driverPostgre = "org.postgresql.Driver"
    val sourcePostgreSchema = "labdata"
    val sourcePostgreDomainCats = "domain_cats"
    val finalPostgreSchema = "denis_lomakov"
    val finalPostgreTable = "clients"

    // HDFS
    val sourceHdfsLogs = "hdfs:///labs/laba03/weblogs.json"

    // ElasticSearch
    val portElastic = "9200"
    val sourceElastcVisits = "visits"

    // Spark-сессия
    val spark = SparkSession.builder()
      .master("yarn")
      .appName("denis_lomakov_lab03")
      .getOrCreate()

    // параметры подключения к Cassandra
    spark.conf.set("spark.cassandra.connection.host", host)
    spark.conf.set("spark.cassandra.connection.port", portCassandra)
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

    // загружаем данные о клиентах из Cassandra
    val clients: DataFrame = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sourceCassandraClients,"keyspace" -> sourceCassandraKeyspace))
      .load()

    // определяем возрастные категории клиентов
    val cat_clients = clients.withColumn("age_cat",
        when((col("age")>=18)&&(col("age")<=24),lit("18-24"))
          .when((col("age")>=25)&&(col("age")<=34),lit("25-34"))
          .when((col("age")>=35)&&(col("age")<=44),lit("35-44"))
          .when((col("age")>=45)&&(col("age")<=54),lit("45-54"))
          .otherwise(lit(">=55")))
      .select("uid", "gender", "age_cat")

    // загружаем логи посещения интернет магазина из ElasticSearch
    val visitsElastic: DataFrame = spark.read
      .format("org.elasticsearch.spark.sql")
      .options(Map("es.read.metadata" -> "true",
        "es.nodes.wan.only" -> "true",
        "es.port" -> portElastic,
        "es.nodes" -> host,
        "es.net.ssl" -> "false"))
      .load(sourceElastcVisits)
      .repartition(2,col("event_type"))

    // загружаем информацию о тематических категориях веб-сайтов из PostgreSQL
    val webCategory: DataFrame = spark.read
      .format("jdbc")
      .options(Map("url" -> s"$urlPostgre/$sourcePostgreSchema",
        "dbtable" -> sourcePostgreDomainCats,
        "user" -> loginPostgre,
        "password" -> password,
        "driver" -> driverPostgre))
      .load()

    // загружаем логи посещения сторонних веб-сайтов пользователями, приобретенные у вендора.
    val logs: DataFrame = spark.read.json(sourceHdfsLogs)
      .select(col("uid"), explode(col("visits")))
      .select("uid", "col.*")
      .where(col("uid").isNotNull)

    // определяем функцию для декодирования URL
    val urlDecoder = udf { (url: String) =>
      Try(URLDecoder.decode(url, "UTF-8")) match {
        case Success(url: String) => url
        case Failure(exc) => ""
      }
    }

    // декодируем URL в полученном df, извлекаем домен
    val pattern: String = "([^:\\/\\n?]+)/?()"
    val groupId: Int = 1
    val logsDecocedUrl = logs.select(col("uid"), urlDecoder(col("url")).as("decoded_url"))
      .where(!(col("decoded_url") === ""))
      .withColumn("tmp_domain", regexp_replace(col("decoded_url"), "^https?://(www.)?", ""))
      .withColumn("domain", regexp_extract(col("tmp_domain"), pattern, groupId))
      .select("uid", "domain")

    // определяем категории посещенных веб сайтов по доменам и считаем количество посещений по категориям
    val webSitesCategory = logsDecocedUrl.join(webCategory, Seq("domain"), "inner")
      .select("uid", "category")
      .withColumn("web_cat", concat(lit("web_"), regexp_replace(regexp_replace(lower(col("category")), " ", "_"), "-", "_")))
      .groupBy("uid").pivot("web_cat").count
      .na.fill(value=0)
      .repartition(5,col("uid"))

    // добавляем к пользователям инфо о посещении различных категорий веб сайтов
    val webSitesCategoryWithClients = cat_clients.join(webSitesCategory,
      Seq("uid"), "left")

    // считаем количество просмотров различных категорий интернет магазина
    val shopLogs = visitsElastic.filter(col("uid").isNotNull)
      .withColumn("shop_cat", concat(lit("shop_"),regexp_replace(regexp_replace(lower(col("category")), " ", "_"), "-", "_")))
      .select("uid", "shop_cat")
      .groupBy("uid").pivot("shop_cat").count
      .na.fill(value=0)
      .repartition(5,col("uid"))

    // добавляем к пользователям инфо о просмотре различных категорий интернет магазина
    val shopLogsWithCategory = cat_clients.join(shopLogs, Seq("uid"), "left")

    // объединяем данные о просмотренных/приобретенных категориях товаров собственного интернет магазина и сторонних вэб сайтов
    val finalTable = shopLogsWithCategory.join(webSitesCategoryWithClients, Seq("uid", "gender", "age_cat"), "outer")
      .na.fill(value=0)

    // сохраняем финальную таблицу
    finalTable.write
      .format("jdbc")
      .option("url", f"$urlPostgre/$finalPostgreSchema")
      .option("dbtable", finalPostgreTable)
      .option("user", loginPostgre)
      .option("password", password)
      .option("driver", driverPostgre)
      .mode("overwrite")
      .save()

    // предоставляем права чекеру на таблицу
    val query = s"GRANT SELECT on clients to labchecker2"
    val connector = DriverManager.getConnection(f"$urlPostgre/$finalPostgreSchema?user=$loginPostgre&password=$password")
    val resultSet = connector.createStatement.execute(query)
  }
}