import org.apache.spark.sql.SparkSession

object Streaming {
  def main(args: Array[String]): Unit = {

    // Setup Contexts
    val spark = SparkSession.builder()
      .master("local")
      .appName("Streaming")
      .getOrCreate()

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "tempretureSim")
      .load()

    df.printSchema()
    // [root@sandbox-hdp ~]# spark-submit --class Streaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --master local[*] ./hellosparkstreaming_2.12-0.1.jar
    /*
    root
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    */

    import spark.implicits._
    val data = df.selectExpr("CAST(key AS String)", "CAST(value AS String)").as[(String, String)]
    data.printSchema()
    /*root
    |-- key: string (nullable = true)
    |-- value: string (nullable = true)
    */

    val values = data.select("value")
    val query = values.writeStream.format("console").start()

    query.awaitTermination()
  }
}
