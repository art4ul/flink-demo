package com.art4ul.flink.demo.mappers

import com.art4ul.flink.demo.entity.{Add, Cmd, Delete}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

import scala.util.Try

object CmdMapper extends FlatMapFunction[String, Cmd] {

  override def flatMap(msg: String, out: Collector[Cmd]): Unit = {
    Try {
      val cmd = msg.split("\\s+").toList

      cmd match {
        case "add" :: deviceId :: userId :: _ =>
          Add(deviceId, userId)

        case "del" :: deviceId :: _ =>
          Delete(deviceId)
      }
    }.foreach { c =>
      out.collect(c)
    }
  }
}
