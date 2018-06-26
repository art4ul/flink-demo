package com.art4ul.flink.demo.mappers

import com.art4ul.flink.demo.entity._
import org.apache.flink.api.common.functions.MapFunction

object SensorMetricMapper extends MapFunction[Temp, Metric] {
  override def map(msg: Temp): Metric = {
    Metric(s"${msg.deviceId}", msg.value, msg.time)
  }
}


object UserMetricMapper extends MapFunction[UserSensor, Metric] {
  override def map(msg: UserSensor): Metric = {
    Metric(s"${msg.userId}.${msg.deviceId}", msg.value, msg.time)
  }
}
