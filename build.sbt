import AssemblyKeys._

name := "SparkStreamingKafka"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.2.0"
//
//libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.2.0"
//
//libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"
//    "javax.servlet" %% "javax.servlet-api" % "3.0.1" % "provided",
//libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0"




libraryDependencies ++= {
  val sparkVersion = "1.2.0"
  Seq(
    "org.apache.spark" % "spark-streaming_2.10" % "1.2.1",
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
    "org.apache.spark" % "spark-sql_2.10" % "1.2.0",
    "org.codehaus.jettison" % "jettison" % "1.3.5"
  )
}

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1"

assemblySettings

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}