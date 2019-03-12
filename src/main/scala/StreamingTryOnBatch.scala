import org.apache.spark.sql.SparkSession

object StreamingTryOnBatch {

  def main(args: Array[String]) {

    // Setup Contexts
    val spark = SparkSession.builder()
      .master("local")
      .appName("Streaming")
      .getOrCreate()

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    // hdfs:///tmp/data/streaming_assignment/tempSimData.json
    val df = spark.read.json("src/main/resources/tempSimData.json")
    df.printSchema()
    df.show()
    val message = df.first()
    print(message)
    print(message.get(0))

    import spark.implicits._
    val flatDf = df.select($"server", $"timestamp", $"sensors.*")
    flatDf.show()

    // 1. Write all events to hdfs
    // hdfs:///tmp/data/streaming_assignment/tempSimDataOutput
    // flatDf.write.json("C:\\Users\\ketan\\source\\root\\HelloSparkStreaming\\tmp\\tempSimDataOutput")

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

    invalid.show()

    // // hdfs:///tmp/data/streaming_assignment/tempSimDataOutput
    // flatDf.write.json("C:\\Users\\ketan\\source\\root\\HelloSparkStreaming\\tmp\\tempSimDataInvalidOutput")

    // 3. average of first two sensors
    val sensorAvg = flatDf
      .filter($"sensor1" > 0)
      .filter($"sensor2">0)
      .groupBy($"server")
      .avg("sensor1","sensor2")

    sensorAvg.show()

   /* val validSensor1 = flatDf
      .filter($"sensor1" > 0)
      .groupBy($"server")
      .avg("sensor1")

    validSensor1.show()

    val validSensor2 = flatDf
      .filter($"sensor2" > 0)
      .groupBy($"server")
      .avg("sensor2")

    validSensor2.show()

    val sensorAvg = validSensor1.join(validSensor2, validSensor1("server") === validSensor2("server"))
        .select(validSensor1("server"), validSensor1("avg(sensor1)"), validSensor2("avg(sensor2)"))

    sensorAvg.show()*/

  }
}
