import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext, Duration}
import weblog.WeblogAnalysizer
import org.apache.spark.streaming.StreamingContext._
import scala.math._
import scala.math._

/**
 * Created by admin on 2015/4/12.
 */
object KafkaCountTotals {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  val computeRunningSum = (values: Seq[Long], state: Option[Long]) => {
    val currentCount = values.foldLeft(0L)(_ + _)
    val previousCount = state.getOrElse(0L)
    Some(currentCount + previousCount)
  }

  val runningCount = new AtomicLong(0)
  val runningSum = new AtomicLong(0)
  val runningMin = new AtomicLong(Long.MaxValue)
  val runningMax = new AtomicLong(Long.MinValue)

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    // StreamingExamples.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads,output) = args   //输入参数命名

    val sparkConf = new SparkConf().setAppName("KafkaWordCount")

    val sc = new SparkContext(sparkConf)

    val ssc =  new StreamingContext(sc, Seconds(3))   //每两秒一批数据

    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD   //隐式转换

    ssc.checkpoint("/home/hadoop/hadoop/kafka/checkpoint")
    //    ssc.checkpoint("f://checkpoint")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lineDstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)   //获取输入流
    val weblogDstream = lineDstream.map(WeblogAnalysizer.parseLogLine).cache()    //输入对象流


    val contentSizesDStream = weblogDstream.map(log => log.contentSize).cache()
    contentSizesDStream.foreachRDD(rdd => {
      val count = rdd.count()
      if (count > 0) {
        runningSum.getAndAdd(rdd.reduce(_ + _))
        runningCount.getAndAdd(count)
        runningMin.set(min(runningMin.get(), rdd.min()))
        runningMax.set(max(runningMax.get(), rdd.max()))
      }
      if (runningCount.get() == 0) {
        println("-------------------Content Size Avg: -, Min: -, Max: -")
      } else {
        println("-------------------Content Size Avg: %s, Min: %s, Max: %s".format(
          runningSum.get() / runningCount.get(),
          runningMin.get(),
          runningMax.get()
        ))
      }
    })

    // Compute Response Code to Count.
    val responseCodeCountDStream = weblogDstream
      .map(log => (log.responseCode, 1L))
      .reduceByKey(_ + _)
    val cumulativeResponseCodeCountDStream = responseCodeCountDStream
      .updateStateByKey(computeRunningSum)
    cumulativeResponseCodeCountDStream.foreachRDD(rdd => {
      val responseCodeToCount = rdd.take(100)
      println(s"""------------Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")
    })

    val ipAddressDStream = weblogDstream
      .map(log => (log.ipAddress, 1L))
      .reduceByKey(_ + _)
      .updateStateByKey(computeRunningSum)
      .filter(_._2 > 10)
      .map(_._1)
    ipAddressDStream.foreachRDD(rdd => {
      val ipAddresses = rdd.take(100)
      println(s"""---------------IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")
    })

    val endpointCountsDStream = weblogDstream
      .map(log => (log.endpoint, 1L))
      .reduceByKey(_ + _)
      .updateStateByKey(computeRunningSum)
    endpointCountsDStream.foreachRDD(rdd => {
      val topEndpoints = rdd.top(10)(OrderingUtils.SecondValueLongOrdering)
      println(s"""-------------Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
