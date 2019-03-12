import org.apache.spark.{SparkConf, SparkContext}

object HelloScala2 {
  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    // hdfs:///tmp/data/shakespeare.txt
    // src/main/resources/shakespeare.txt
    val textFile = sc.textFile("hdfs:///tmp/data/shakespeare.txt")

    //word count
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    //counts.foreach(println)
    //print("Total words: " + counts.count())
    // hdfs:///tmp/data/shakespeareWordCount
    // C:\Users\ketan\source\root\HelloSparkStreaming\tmp\shakespeareWordCount
    counts.saveAsTextFile("hdfs:///tmp/data/shakespeareWordCount")
  }
}
