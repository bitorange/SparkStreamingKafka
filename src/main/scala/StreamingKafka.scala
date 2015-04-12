import org.apache.spark.{SparkContext, SparkConf}
import weblog.WeblogAnalysizer

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.sql.SQLContext
/**
 * Created by admin on 2015/4/7.
 */
object KafkaAction {
  //没有窗口，直接batch操作
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

//   ssc.checkpoint("/home/hadoop/hadoop/kafka/checkpoint")
//    ssc.checkpoint("f://checkpoint")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lineDstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)   //获取输入流
    val weblogDstream = lineDstream.map(WeblogAnalysizer.parseLogLine).cache()

    weblogDstream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        println("No access com.databricks.app.logs received in this time interval")
      } else {
        accessLogs.printSchema()
        accessLogs.registerTempTable("accesslog")

        // Calculate statistics based on the content size.
        val contentSizeStats = sqlContext
          .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM accesslog")
          .first()
        println("Content Size Avg: %s, Min: %s, Max: %s".format(
          contentSizeStats.getLong(0) / contentSizeStats.getLong(1),
          contentSizeStats(2),
          contentSizeStats(3)))

        // Compute Response Code to Count.
        val responseCodeToCount = sqlContext
          .sql("SELECT responseCode, COUNT(*) FROM accesslog GROUP BY responseCode")
          .map(row => (row.getInt(0), row.getLong(1)))
          .take(1000)
        println(s"""Response code counts: ${responseCodeToCount.mkString("[", ",", "]")}""")

        // Any IPAddress that has accessed the server more than 10 times.
        val ipAddresses =sqlContext
          .sql("SELECT ipAddress, COUNT(*) AS total FROM accesslog GROUP BY ipAddress HAVING total > 10")
          .map(row => row.getString(0))
          .take(100)
        println(s"""IPAddresses > 10 times: ${ipAddresses.mkString("[", ",", "]")}""")

        val topEndpoints = sqlContext
          .sql("SELECT endpoint, COUNT(*) AS total FROM accesslog GROUP BY endpoint ORDER BY total DESC LIMIT 10")
          .map(row => (row.getString(0), row.getLong(1)))
          .collect()
        println(s"""Top Endpoints: ${topEndpoints.mkString("[", ",", "]")}""")

        val contentRDD = sqlContext
        .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM accesslog")
        println("--------------------------------------------储存中----------------------------------------------------------")
        contentRDD.saveAsTextFile(output+System.currentTimeMillis())       // 这里只输出日志一次，为什么呢？？ 通过output那里加一个currentTimeMillis
        val total = contentSizeStats.union(responseCodeToCount).union(ipAddresses)
        total.foreach(println(_))
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }


 /* def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    // StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("/home/hadoop/hadoop/kafka/checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

    val jsonf = lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, Any]])
    jsonf.filter(l => l("lineno")==75).window(Seconds(30)).foreachRDD( rdd => {
      rdd.foreach( r => {
        println(r("path"))
      })
    })
  }*/
}


