package org.linc.spark.sparkstreaming

import java.util.Properties
import scala.collection.immutable.HashMap

import scala.io.Source

/**
 * Created by admin on 2015/4/13.
 */

object GlobalConf{
  //读取配置文件
  val properties: Properties = System.getProperties
  val path: String = properties.getProperty("user.dir")
  val reader = Source.fromFile(path+"/SparkStreamingAndSql.conf").getLines()
  var map = HashMap[String, String]();
  for(lines <- reader if lines.startsWith("#") == false;if lines != "" ){
    val keyValues = lines.split("=")
    map += (keyValues(0) -> keyValues(1))
  }
  //配置文件中变量赋值
  val zkQuorum = map.get("zkQuorum")
  val group = map.get("group")
  val topics = map.get("topics")
  val numThreads = map.get("numThreads")
  val sqlOutPutPath = map.get("sqlOutPutPath")
  val inputFormatFilePath = map.get("inputFormatFilePath")
  val outputFormatFilePath = map.get("outputFormatFilePath")
  val rulesFilePath = map.get("rulesFilePath")
  val outputPath = map.get("outputPath")
  val batchInterval= map.get("batchInterval")
  val windowInterval = map.get("windowInterval")
  val slideInterval = map.get("slideInterval")
  val sql = map.get("sql")
}
