import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext, Duration}
import weblog.WeblogAnalysizer

import scala.collection.immutable.HashMap
import scala.io.Source

/**
 * this class is a test to use kafka, Spark Streaming, Spark SQL and HDFS in which the data is traced by kafka ,
 * procesded by Spark Streaming, SQLed by Spark SQL and saved in HDFS finally.
 * Created by xwc on 2015/4/12.
 */

class SmallTest{
  //读取配置文件
  val properties: Properties = System.getProperties
  val path: String = properties.getProperty("user.dir")
  val reader = Source.fromFile(path+"/SparkStreamingAndSql.conf").getLines()
  var map = HashMap[String, String]();
  for(lines <- reader if lines.startsWith("#") == false){
    val keyValues = lines.split("=")
    map += (keyValues(0) -> keyValues(1))
  }
  val zkQuorum = map.get("zkQuorum")
  val group = map.get("group")
  val topics = map.get("topics")
  val numThreads = map.get("numThreads")
  val outPutPath = map.get("outPutPath")
}

object SmallTest {
  //使用窗口操作
   val WINDOW_LENGTH = new Duration(10 * 1000)
   val SLIDE_INTERVAL = new Duration(6 * 1000)
  def main(args: Array[String]) {
    //定义类，初始化配置变量
    val smallTest = new SmallTest
    //参数命名
    //spark系统上下文
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc = new SparkContext(sparkConf)
    val ssc =  new StreamingContext(sc, Seconds(2))   //batch interval 2s
    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD

    //读取输入数据
    val topicMap = smallTest.topics.get.split(",").map((_,smallTest.numThreads.get.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, smallTest.zkQuorum.get, smallTest.group.get, topicMap).map(_._2)
    val accessLogsDStream = lines.map(WeblogAnalysizer.parseLogLine).cache()
    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    //数据处理
    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        println("No access com.databricks.app.logs received in this time interval")
      } else {
        accessLogs.printSchema()
        accessLogs.registerTempTable("accesslog")

        // Calculate statistics based on the content size.
        val contentSizeStats = sqlContext
          .sql("SELECT SUM(contentSize), COUNT(*), MIN(contentSize), MAX(contentSize) FROM accesslog")    //sql操作

        println("--------------------------------------------储存中----------------------------------------------------------")
        contentSizeStats.collect().foreach(println)
        contentSizeStats.saveAsTextFile(smallTest.outPutPath.get+System.currentTimeMillis())
      }
    })
  }
}
