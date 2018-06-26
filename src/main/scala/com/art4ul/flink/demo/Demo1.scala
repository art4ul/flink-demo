package com.art4ul.flink.demo

import com.art4ul.flink.demo.entity.Metric
import com.art4ul.flink.demo.source.DataGen
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object Demo1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new DataGen()).print()

    env.execute("Flink Demo 1")
  }

}
