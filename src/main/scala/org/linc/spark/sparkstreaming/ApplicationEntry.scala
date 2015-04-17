package org.linc.spark.sparkstreaming

import java.util

import org.apache.spark.sql._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._


/**
 * The Entry of Application
 * Created by xwc on 2015/4/12.
 */

object ApplicationEntry {
  // 窗口大小
  val WINDOW_LENGTH = new Duration(GlobalConf.windowInterval.get.toLong)
  // 滑动间隔
  val SLIDE_INTERVAL = new Duration(GlobalConf.slideInterval.get.toLong)
  // 输入输出格式
  val inputAndOutputFormat = new InputAndOutputFormat()
  // 转换规则
  val rules = new Rules(inputAndOutputFormat)

  /**
   * 程序入口
   * @param args 参数列表
   */
  def main(args: Array[String]) {
    /* 初始化 */
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(GlobalConf.batchInterval.get.toLong))
    val sqlContext = new SQLContext(sc)

    /* 从 Kafka 中读取输入数据 */
    val topicMap = GlobalConf.topics.get.split(",").map((_, GlobalConf.numThreads.get.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, GlobalConf.zkQuorum.get, GlobalConf.group.get, topicMap).map(_._2)

    /* 字段规则 */
    val splitRDD = lines.map(x => inputAndOutputFormat.splitInputIntoHashMap(x))
    val inputArrayRDD = lines.map(x => new util.ArrayList(inputAndOutputFormat.splitInputIntoHashMap(x).values()))
    val outputArrayRDD = splitRDD.map(x => rules.applyRules(x))

    /* 输出结果到文件系统当中 */
    outputArrayRDD.foreachRDD(allResult => {
      if (allResult.count() > 0) {
        val finalRDD = allResult.map(result => result) // Nothing to do
        finalRDD.collect().foreach(println)
        finalRDD.saveAsTextFile(GlobalConf.outputPath.get)
      }
    })

    /* 创建 Schema */
    import org.apache.spark.sql._
    val inputSchema =
      StructType(inputAndOutputFormat.getInputFormat.keySet().toArray().map(key => {
        // println("Key =" + key)
        StructField(key.toString,
          getType(inputAndOutputFormat.getInputFormat.get(key)), true)
      }).toSeq) // input 表 Schema
    val outputSchema =
      StructType(inputAndOutputFormat.getOutputFormat.keySet().toArray().map(key =>
        StructField(key.toString,
          getType(inputAndOutputFormat.getOutputFormat.get(key)), true))) // output 表 Schema

    /* 窗口 */
    val tmpInputTupleRDD = inputArrayRDD.map(x => (1, x))
    val tmpOutputTupleRDD = outputArrayRDD.map(x => (2, x))
    val combinedInputAndOutputRDD = tmpInputTupleRDD.union(tmpOutputTupleRDD)
    // combinedInputAndOutputRDD.foreach(x => println(x.collect().foreach(println)))
    val windowDStream = combinedInputAndOutputRDD.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    /* 执行 SQL 查询 */
    windowDStream.foreachRDD(originalTupleRDD => {
      if (originalTupleRDD.count() == 0) {
        println("No data in this time interval")
      }
      else {
        // 注册 input 表并往里插入数据
        val inputRowRDD = originalTupleRDD.filter(x => x._1 == 1).map(rowTuple => {
          val row = Row.fromSeq(rowTuple._2)
          row
        })
        val inputSchemaRDD = sqlContext.applySchema(inputRowRDD, inputSchema)
        inputSchemaRDD.registerTempTable("input")

        // 注册 output 表并往里插入数据
        val outputRowRDD = originalTupleRDD.filter(x => x._1 == 2).map(rowTuple => {
          val row = Row.fromSeq(rowTuple._2)
          row
        })
        val outputSchemaRDD = sqlContext.applySchema(outputRowRDD, outputSchema)
        outputSchemaRDD.registerTempTable("output")

        // 执行 SQL 查询
        val contentSizeStats = sqlContext.sql(GlobalConf.sql.get)
        println("SQL Query Result: ")
        println("Count = " + contentSizeStats.count)
        contentSizeStats.collect().foreach(println)
        contentSizeStats.saveAsTextFile(GlobalConf.sqlOutPutPath.get)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 根据字符串获取相应的字段类型
   * @param t 类型
   * @return 对应的字段类型
   */
  def getType(t: String): DataType = {
    if (t == "String") {
      return StringType
    }
    else if (t == "Float" || t == "float") {
      return FloatType
    }
    else if (t == "Double" || t == "double") {
      return DoubleType
    }
    else if (t == "Integer" || t == "int") {
      return IntegerType
    }
    else if (t == "Long" || t == "long") {
      return LongType
    }
    else if (t == "Boolean" || t == "boolean") {
      return BooleanType
    }
    else if (t == "Byte" || t == "byte") {
      return ByteType
    }
    else if (t == "Short" || t == "short") {
      return ShortType
    }
    StringType
  }
}
