package com.art4ul.flink.demo.window

import com.art4ul.flink.demo.entity.{Metric, UserSensor, Temp}
import org.apache.flink.api.common.functions.AggregateFunction

case class MeanState(sum: Double = 0,
                     count: Long = 0,
                     minTime: Long = Long.MaxValue,
                     userId: String = "")

class MeanAggregate extends AggregateFunction[UserSensor, MeanState, Metric] {
  override def createAccumulator(): MeanState = MeanState()

  override def add(value: UserSensor, accumulator: MeanState): MeanState = {
    MeanState(
      sum = accumulator.sum + value.value,
      count = accumulator.count + 1,
      minTime = Math.min(value.time, accumulator.minTime),
      userId = value.userId
    )
  }

  override def getResult(accumulator: MeanState): Metric = {
    val mean = accumulator.sum / accumulator.count
    Metric(s"mean.${accumulator.userId}", mean, accumulator.minTime)
  }

  override def merge(a: MeanState, b: MeanState): MeanState = {
    MeanState(
      sum = a.sum + b.sum,
      count = a.count + b.count,
      minTime = Math.min(a.minTime, b.minTime),
      userId = a.userId
    )
  }
}

