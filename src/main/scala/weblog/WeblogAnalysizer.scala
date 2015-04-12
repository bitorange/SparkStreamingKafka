package weblog

/**
 * Created by admin on 2015/4/11.
 */
case class WeblogAnalysizer(ipAddress: String, clientIdentd: String,
                           userId: String, dateTime: String, method: String,
                           endpoint: String, protocol: String,
                           responseCode: Int, contentSize: Long) {
 /* override def toString(): String ={
  }*/

}


object WeblogAnalysizer{
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  def parseLogLine(log: String): WeblogAnalysizer = {
    val res = PATTERN.findFirstMatchIn(log)
    if(res.isEmpty){
      throw new RuntimeException("can't parse log line:" + log)
    }
    val m = res.get
    WeblogAnalysizer(m.group(1),m.group(2), m.group(3), m.group(4),
      m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong)
  }
}