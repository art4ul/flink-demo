package com.art4ul.flink.demo

import com.art4ul.flink.demo.entity.Metric
import com.art4ul.flink.demo.mappers.SensorMetricMapper
import com.art4ul.flink.demo.sink.InfluxSink
import com.art4ul.flink.demo.source.DataGen
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Demo2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.addSource(new DataGen()).keyBy(_.deviceId)
    val sink = new InfluxSink

    src.map(SensorMetricMapper).addSink(sink)

    env.execute("Flink Demo 2")
  }

}
