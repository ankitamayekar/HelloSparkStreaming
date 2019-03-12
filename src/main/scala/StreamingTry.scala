import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingTry {
  def main(args: Array[String]) {

    // Setup Contexts
    val sparkConfig = new SparkConf()
      .setAppName("Word Counter")
      .setMaster("local[2]")
    val sparkContext = new SparkContext(sparkConfig)
    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    //streamingContext.checkpoint("/tmp/data/streamingCheckpoint")

    // Setup Kafka as input
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "tempretureSim_cg",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("tempretureSim")

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    //stream.map(record => (record.key, record.value))
    stream.print()
  }
}
