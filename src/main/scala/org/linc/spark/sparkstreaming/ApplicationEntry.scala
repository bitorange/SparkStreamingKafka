package org.linc.spark.sparkstreaming

import java.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


/**
 * this class is a test to use kafka, Spark Streaming, Spark SQL and HDFS in which the data is traced by kafka ,
 * processed by Spark Streaming, SQLed by Spark SQL and saved in HDFS finally.
 * Created by xwc on 2015/4/12.
 */


object ApplicationEntry {
  //使用窗口操作
  val WINDOW_LENGTH = new Duration(GlobalConf.windowInterval.get.toLong)
  val SLIDE_INTERVAL = new Duration(GlobalConf.slideInterval.get.toLong)
  val inputAndOutputFormat = new InputAndOutputFormat()
  // 输入输出格式
  val rules = new Rules(inputAndOutputFormat) // 转换规则

  def main(args: Array[String]) {
    /* 初始化 */
    // Spark 系统上下文 ß
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(GlobalConf.batchInterval.get.toLong))
    val sqlContext = new SQLContext(sc)

    // 读取输入数据
    val topicMap = GlobalConf.topics.get.split(",").map((_, GlobalConf.numThreads.get.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, GlobalConf.zkQuorum.get, GlobalConf.group.get, topicMap).map(_._2)

    /* 字段规则 */
    // 应用转换规则
    val splitRDD = lines.map(x => inputAndOutputFormat.splitInputIntoHashMap(x))
    val resultRDD = splitRDD.map(x => rules.applyRules(x))

    // 输出结果到 HDFS
    resultRDD.foreachRDD(allResult => {
      if (allResult.count() > 0) {
        val finalRDD = allResult.map(result => Rules.resultToStr(result))
        finalRDD.collect().foreach(println)
        finalRDD.saveAsTextFile(GlobalConf.outputPath.get)
      }
    })

    /* SQL 查询 */
    // 创建 Schema
    import org.apache.spark.sql._
    def getType(t: String): DataType = {
      if (t == "String") {
        StringType
      }
      else if (t == "Float") {
        FloatType
      }
      else if (t == "Double") {
        DoubleType
      }
      else if (t == "Integer") {
        IntegerType
      }
      StringType
    };
    val inputValueRDD = lines.map(x => new util.ArrayList(inputAndOutputFormat.splitInputIntoHashMap(x).values()))

    val inputSchema =
      StructType(inputAndOutputFormat.getInputFormat.keySet().map(key => StructField(key,
        getType(inputAndOutputFormat.getInputFormat.get(key)), true)).toSeq)

    // 窗口
    val windowInputDStream = inputValueRDD.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    // SQL 查询
    windowInputDStream.foreachRDD(allInputValue => {
      if (allInputValue.count() == 0) {
        println("No input data received in this time interval")
      } else {
        val rowRDD = allInputValue.map(result => {
          val row = Row.fromSeq(result.toIndexedSeq)
          row
        })

        val schemaRDD = sqlContext.applySchema(rowRDD, inputSchema)
        schemaRDD.registerTempTable("input")
        // val contentSizeStats = sqlContext.sql(GlobalConf.sql.get) // SQL 操作
        // contentSizeStats.collect().foreach(println)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
