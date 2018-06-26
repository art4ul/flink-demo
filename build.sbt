name := "flink-workshop"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  val flinkVersion = "1.5.0"
  Seq(
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
    "org.influxdb" % "influxdb-java" % "2.10",
    "org.slf4j" % "slf4j-nop" % "1.7.25"
  )
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

mainClass in assembly := Some("com.art4ul.flink.demo.Demo1")
