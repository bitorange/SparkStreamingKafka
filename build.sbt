//import AssemblyKeys._

name := "SparkStreamingKafka"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVersion = "1.2.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "javax.servlet" % "javax.servlet-api" % "3.0.1",
    "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
  )
}

//assemblySettings
//
//mergeStrategy in assembly := {
//  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
//  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
//  case "log4j.properties"                                  => MergeStrategy.discard
//  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
//  case "reference.conf"                                    => MergeStrategy.concat
//  case _                                                   => MergeStrategy.first
//}