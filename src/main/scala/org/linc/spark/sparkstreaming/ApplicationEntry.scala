package org.linc.spark.sparkstreaming

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.sql._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
 * The Entry of Application
 * Author: xwc / ihainan
 */
object ApplicationEntry {
  /* Spark Streaming 相关参数 */
  var WINDOW_LENGTH = new Duration(0)
  var SLIDE_INTERVAL = new Duration(0)
  var BATCH_INTERVAL = Seconds(0)
  var OUTPUT_PATH = ""
  var INPUT_FORMAT = "separator"
  var SEPARATOR = "\t"

  /* ZooKeeper 相关参数 */
  var ZOOKEEPER_TOPICS = ""
  var ZOOKEEPER_NUM_THREADS = 1
  var ZOOKEEPER_GROUP = ""
  var ZOOKEEPER_URL = ""

  /* SQL 相关 */
  var SQL_COMMAND = ""
  var SQL_OUTPUT_PATH = ""
  var ENABLE_EXTRA_SQL = false
  var EXTRA_SQL_COMMAND = ""

  /**
   * 解析配置文件，获取程序配置
   * @param args 程序运行参数
   */
  def readConfigureFile(args: Array[String]): Unit = {
    GlobalVar.parseArgs(args);
    ApplicationEntry.WINDOW_LENGTH = new Duration((GlobalVar.configMap.get("stream.window.length")).toLong)
    ApplicationEntry.SLIDE_INTERVAL = new Duration((GlobalVar.configMap.get("stream.window.slide")).toLong)
    ApplicationEntry.BATCH_INTERVAL = Seconds((GlobalVar.configMap.get("stream.batchInterval")).toLong)
    ApplicationEntry.OUTPUT_PATH = GlobalVar.configMap.get("stream.output.savePath")

    ApplicationEntry.INPUT_FORMAT = GlobalVar.configMap.get("stream.input.format")
    ApplicationEntry.SEPARATOR = GlobalVar.configMap.get("stream.input.separator")

    ApplicationEntry.ZOOKEEPER_URL = GlobalVar.configMap.get("zookeeper.url")
    ApplicationEntry.ZOOKEEPER_TOPICS = GlobalVar.configMap.get("zookeeper.topics")
    ApplicationEntry.ZOOKEEPER_NUM_THREADS = (GlobalVar.configMap.get("zookeeper.numThreads")).toInt
    ApplicationEntry.ZOOKEEPER_GROUP = GlobalVar.configMap.get("zookeeper.group")

    ApplicationEntry.SQL_COMMAND = GlobalVar.configMap.get("stream.sql.command")
    ApplicationEntry.SQL_OUTPUT_PATH = GlobalVar.configMap.get("stream.sql.savePath")

    ApplicationEntry.EXTRA_SQL_COMMAND = GlobalVar.configMap.get("stream.extraSQL.command")
    ApplicationEntry.ENABLE_EXTRA_SQL = GlobalVar.configMap.get("stream.extraSQL.enable").toBoolean
  }

  def genMapper[A, B](f: A => B): A => B = {
    val locker = com.twitter.chill.MeatLocker(f)
    x => locker.get.apply(x)
  }

  /**
   * 程序入口
   * @param args 参数列表
   */
  def main(args: Array[String]) {
    /* 初始化 */
    readConfigureFile(args)
    val inputAndOutputFormat = new InputAndOutputFormat()
    val rules = new Rules(inputAndOutputFormat)

    /* 初始化 Spark */
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, ApplicationEntry.BATCH_INTERVAL)
    val sqlContext = new SQLContext(sc)

    /* 从 Kafka 中读取输入数据 */
    val topicMap = ApplicationEntry.ZOOKEEPER_TOPICS.split(",").map((_, ApplicationEntry.ZOOKEEPER_NUM_THREADS)).toMap
    val lines = KafkaUtils.createStream(ssc, ApplicationEntry.ZOOKEEPER_URL, ApplicationEntry.ZOOKEEPER_GROUP, topicMap).map(_._2)
    lines.foreachRDD(rdd => {
      rdd.foreach(println)
    })

    /* 字段规则 */
    val splitLines = lines.map(x => inputAndOutputFormat.splitInputIntoHashMap(x))
    val inputArrayDStream = lines.map(x => new util.ArrayList(inputAndOutputFormat.splitInputIntoHashMap(x).values()))
    val outputArrayDStream = splitLines.map(x => rules.applyRules(x))
    var finalOutputArrayDStream = outputArrayDStream // 考虑是否带额外 SQL 的情况

    /* 创建 Schema */
    import org.apache.spark.sql._

    /* 额外的 SQL 查询 */
    if (ENABLE_EXTRA_SQL) {
      // 创建 Schema
      val extraSQLInputSchema =
        StructType(inputAndOutputFormat.getOutputFormat.keySet().toArray().map(key =>
          StructField(key.toString,
            getType(inputAndOutputFormat.getOutputFormat.get(key)), true)))

      // 实际上，这里仅会有一个或者零个 RDD
      outputArrayDStream.foreachRDD(rdd => {
        rdd.foreach(x => println("_ " + x))
      })

      finalOutputArrayDStream = outputArrayDStream.transform(rdd => {
        // 注册 extraInput 表并往里插入数据
        rdd.foreach(x => println("_ " + x))
        val extraSQLInputRowRDD = rdd.map(Row.fromSeq(_))
        val extraSQLInputSchemaRDD = sqlContext.applySchema(extraSQLInputRowRDD, extraSQLInputSchema)
        extraSQLInputSchemaRDD.registerTempTable("extraInput")

        val result = sqlContext.sql(ApplicationEntry.EXTRA_SQL_COMMAND)
        println("Extra SQL Query Result: ")
        result.foreach(println(_))
        val r = SchemaRDDToRDD(result).map(x => {
          new util.ArrayList[Object](x.values.toList)
        })
        r
      })

    }

    // TODO: FinalOutputArray
    /* 输出结果到文件系统当中 */
    finalOutputArrayDStream.foreachRDD(allResult => {
      if (allResult.count() > 0) {
        val finalRDD = allResult.map(result => result) // Nothing to do
        finalRDD.collect().foreach(println)
        finalRDD.saveAsTextFile(ApplicationEntry.OUTPUT_PATH)
      }
    })

    /* 常规 SQL 统计*/
    // 创建 Schema
    val inputSchema =
      StructType(inputAndOutputFormat.getInputFormat.keySet().toArray().map(key => {
        // println("Key =" + key)
        StructField(key.toString,
          getType(inputAndOutputFormat.getInputFormat.get(key)), true)
      }).toSeq) // input 表 Schema
    val outputSchema =
      StructType(inputAndOutputFormat.getFinalOutputFormat.keySet().toArray().map(key =>
        StructField(key.toString,
          getType(inputAndOutputFormat.getOutputFormat.get(key)), true))) // output 表 Schema

    /* 窗口 */
    // TODO: FinalOutputFormat, FinalOutputArray
    val tmpInputTupleRDD = inputArrayDStream.map(x => (1, x))
    val tmpOutputTupleRDD = finalOutputArrayDStream.map(x => (2, x))
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
        val contentSizeStats = sqlContext.sql(ApplicationEntry.SQL_COMMAND)
        println("SQL Query Result: ")
        println("Count = " + contentSizeStats.count)
        contentSizeStats.collect().foreach(println)
        contentSizeStats.saveAsTextFile(ApplicationEntry.SQL_OUTPUT_PATH)
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

  def SchemaRDDToRDD(schemaRDD: SchemaRDD): RDD[Map[String, String]] = {
    val types = schemaRDD.schema.fields.map(field => field.dataType.toString)
    val names = schemaRDD.schema.fields.map(field => field.name)
    schemaRDD.map(row => rowToString(row, types.toArray, names.toArray))
  }

  def rowToString(row: Row, types: Array[String], names: Array[String]): Map[String, String] = {
    val values = (0 until row.length).map { i => getValue(types, row, i) }
    (names zip values).toMap
  }

  def getValue(types: Array[String], row: Row, i: Int): String = {
    val dataType = types(i)
    dataType match {
      case "IntegerType" => row.getInt(i).toString()
      case "FloatType" => row.getFloat(i).toString()
      case "LongType" => row.getLong(i).toString()
      case "DoubleType" => row.getDouble(i).toString()
      case "TimestampType" => row.getString(i).toString()
      case "ShortType" => row.getShort(i).toString()
      case "ByteType" => row.getByte(i).toString()
      case "StringType" => row.getString(i).toString()
    }
  }

}
