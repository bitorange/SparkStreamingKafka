import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Seconds}

/**
 * Created by admin on 2015/4/12.
 */
object KafkaOut {
  def main(args:Array[String])
  {
    if (args.length < 5) {
      System.err.println("Usage: KafkaTest     ")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads,output) = args
    val sparkConf = new SparkConf().setAppName("KafkaTest")
    val ssc =  new StreamingContext(sparkConf, Seconds(3))
   // ssc.checkpoint("checkpoint")

    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)
    lines.saveAsTextFiles(output)   //自动输出文件，后缀自动加timeStamp且间隔是batch interval
    ssc.start()
    ssc.awaitTermination()

    //.saveAsTextFile(output)


  }
}
