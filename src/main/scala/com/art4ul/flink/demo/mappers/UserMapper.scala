package com.art4ul.flink.demo.mappers

import com.art4ul.flink.demo.entity._
import scala.collection.JavaConversions._
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInfo, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector


class UserMapper[C <: Cmd] extends RichCoFlatMapFunction[Sensor, C, UserSensor] {

  var mapping: MapState[String, String] = _

  override def open(parameters: Configuration): Unit = {
    val descriptor = new MapStateDescriptor[String, String]("user-mapping",
      TypeInformation.of(classOf[String]),
      TypeInformation.of(classOf[String])
    )
    mapping = getRuntimeContext.getMapState(descriptor)
  }

  override def flatMap1(v: Sensor, out: Collector[UserSensor]): Unit = {
    val user = Option(mapping.get(v.deviceId))
    user.foreach { userId =>
      val result = UserSensor(time = v.time,
        userId = userId,
        deviceId = v.deviceId,
        value = v.value
      )
      out.collect(result)
    }
  }

  override def flatMap2(value: C, out: Collector[UserSensor]): Unit = {
    value match {
      case Add(deviceId,userId) => mapping.put( deviceId, userId )
      case Delete( deviceId) => mapping.remove(deviceId)
    }
  }
}
