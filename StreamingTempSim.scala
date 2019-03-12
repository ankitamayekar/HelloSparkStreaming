import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object StreamingTempSim {
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

    // value schema: { "a": 1, "b": "string" }
    val schema = new StructType()
      .add("server", StringType)
      .add("timestamp", TimestampType)
      .add("sensors",
        new StructType()
          .add("sensor1", IntegerType)
          .add("sensor2", IntegerType)
          .add("sensor3", IntegerType)
          .add("sensor4", IntegerType)
          .add("sensor5", IntegerType)
          .add("sensor6", IntegerType)
          .add("sensor7", IntegerType)
          .add("sensor8", IntegerType)
          .add("sensor9", IntegerType)
          .add("sensor10", IntegerType)
        )

    val values = data.select(from_json($"value", schema) as "jsonValues")
    values.printSchema()

    // copying code from StreamingTryOnBatch.scala
    val flatDf = values.select($"jsonValues.*").select($"server", $"timestamp", $"sensors.*")
    flatDf.printSchema()

    // 1. Write all events to hdfs
    // hdfs:///tmp/data/streaming_assignment/tempSimDataOutput
    // flatDf.write.json("hdfs:///tmp/data/streaming_assignment/tempSimDataOutput")
    /*
    val query1 = flatDf
      .writeStream
      .format("console")
      //.option("checkpointLocation", "file:///tmp/chk_01")
      //.option("path", "hdfs:///user/hadoop/all_stream_01")
      .start()
*/
/*
    // 2. Detect invalid sensor data (0 or -ve) and write to hdfs
    val invalid = flatDf
      .filter($"sensor1" < 0
        || $"sensor2" < 0
        || $"sensor2" < 0
        || $"sensor3" < 0
        || $"sensor4" < 0
        || $"sensor5" < 0
        || $"sensor6" < 0
        || $"sensor7" < 0
        || $"sensor8" < 0
        || $"sensor9" < 0
        || $"sensor10" < 0)
*/
    //invalid.show()

    // // hdfs:///tmp/data/streaming_assignment/tempSimDataOutput
    // flatDf.write.json("C:\\Users\\ketan\\source\\root\\HelloSparkStreaming\\tmp\\tempSimDataInvalidOutput")
    /*
    val query2 = invalid
      .writeStream
      .format("console")
      //.option("checkpointLocation", "hdfs:///tmp/chk_02")
      //.option("path", "hdfs:///tmp/data/streaming_assignment/invalid_stream_01")
      .start()
*/
    // 3. average of first two sensors
    val validSensor1 = flatDf
      .filter($"sensor1" > 0)
      .filter($"sensor2">0)
      .groupBy($"server")
      .avg("sensor1", "sensor2")

    //validSensor1.show()
/*
    val validSensor2 = flatDf
      .filter($"sensor2" > 0)
      .groupBy($"server")
      .avg("sensor2")

    //validSensor2.show()

    val sensorAvg = validSensor1
      .join(validSensor2, validSensor1("server") === validSensor2("server"))
      .select(validSensor1("server"), validSensor1("avg(sensor1)"), validSensor2("avg(sensor2)"))
*/
    /*
    val query3 = validSensor1
      .writeStream
      .format("csv")
      .option("checkpointLocation", "file:///tmp/chk_01")
      .outputMode("update")
      .option("path", "file:///tmp/query3")
      .start()
    */

    val query3 = validSensor1
      //.select($"server" as "key", $"avg(sensor1)" as "value" cast StringType)
      .select($"server" as "key", to_json(struct("avg(sensor1)", "avg(sensor2)")) as "value" cast StringType)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("topic", "avgTempSim")
      .option("checkpointLocation", "file:///tmp/kafkachk")
      .outputMode("update")
      .start()

    //* val query3 = validSensor1
    //      .writeStream
    //      .format("console")
    //      .outputMode("update")
    //      .option("checkpointLocation", "file:///tmp/chk_01")
    //      .option("path", "hdfs:///user/hadoop/all_stream_01")
    //      .start()*//

    //query1.awaitTermination()
    //query2.awaitTermination()
    query3.awaitTermination()
  }
}
