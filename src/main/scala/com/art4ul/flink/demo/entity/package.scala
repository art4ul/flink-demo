package com.art4ul.flink.demo

package object entity {

  case class Sensor(time: Long,
                    deviceId: String,
                    value: Double)

  case class UserSensor(time: Long,
                        userId: String,
                        deviceId: String,
                        value: Double)

  case class Metric(name: String,
                    value: Double,
                    time: Long)

  trait Cmd {
    val deviceId: String
  }

  case class Add(override val deviceId: String, val userId: String) extends Cmd

  case class Delete(override val deviceId: String) extends Cmd

}
