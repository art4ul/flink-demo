package com.art4ul.flink.demo.mappers

import com.art4ul.flink.demo.entity._
import org.apache.flink.api.common.functions.MapFunction

object SensorMetricMapper extends MapFunction[Sensor, Metric] {
  override def map(msg: Sensor): Metric = {
    Metric(s"${msg.deviceId}", msg.value, msg.time)
  }
}


object UserMetricMapper extends MapFunction[UserSensor, Metric] {
  override def map(msg: UserSensor): Metric = {
    Metric(s"${msg.userId}.${msg.deviceId}", msg.value, msg.time)
  }
}
