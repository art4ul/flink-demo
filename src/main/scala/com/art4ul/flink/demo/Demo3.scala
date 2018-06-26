package com.art4ul.flink.demo

import java.util.concurrent.TimeUnit

import com.art4ul.flink.demo.mappers._
import com.art4ul.flink.demo.sink.InfluxSink
import com.art4ul.flink.demo.source.DataGen
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._

object Demo3 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(100,
        Time.of(10, TimeUnit.SECONDS))
    )
    env.enableCheckpointing(1000)

    val src = env.addSource(new DataGen())
    val sink = new InfluxSink

    src.map(SensorMetricMapper).addSink(sink)

    val contorl = env.socketTextStream("localhost", 12345)
      .flatMap(CmdMapper)
      .keyBy(m => m.deviceId)

    val userFlow = src.keyBy(_.deviceId)
      .connect(contorl)
      .flatMap(new UserMapper)
      .map(UserMetricMapper)

    userFlow.addSink(sink)

    env.execute("Flink Demo 3")
  }

}
