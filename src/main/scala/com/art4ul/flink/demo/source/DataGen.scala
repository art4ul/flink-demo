package com.art4ul.flink.demo.source

import com.art4ul.flink.demo.entity._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.watermark.Watermark

case class DataGenState(var t: Double = 0.0)

object DataGen {
  type Gen = (Double => Double)

  case class DeviceDataGen( deviceId: String, gen: Gen)

  val gens: Seq[DeviceDataGen] = Seq(
    DeviceDataGen( "Device1", (x => 24.0 + x)),
    DeviceDataGen( "Device2", (x => 20 * (-Math.sin(x)) + 50)),
    DeviceDataGen( "Device3", (x => 56.0 - x)),
    DeviceDataGen( "Device4", (t => 20 * Math.sin(t) + 50))
  )

  def gen(x: Double): Seq[Temp] = {
    val t = System.currentTimeMillis()
    gens.map { case DeviceDataGen(deviceId, g) =>
      Temp(t, deviceId, g(x))
    }
  }
}

class DataGen extends SourceFunction[Temp] with CheckpointedFunction {

  @transient
  private var checkpointedState: ListState[DataGenState] = _
  private val Step = 0.025

  @volatile private var isRunning: Boolean = true
  @transient var state: DataGenState = _


  import Thread._

  override def run(ctx: SourceFunction.SourceContext[Temp]): Unit = {
    while (isRunning) {
      val time = System.currentTimeMillis()
      val events = DataGen.gen(state.t)
      events.foreach { e =>
        ctx.collectWithTimestamp(e, time)
        sleep(200)
      }
      sleep(500)

      ctx.emitWatermark(new Watermark(time))
      state.t += Step
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    checkpointedState.add(state)
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[DataGenState](
      "source-state",
      TypeInformation.of(new TypeHint[DataGenState]() {})
    )

    checkpointedState = context.getOperatorStateStore.getListState(descriptor)

    val stateIter: Seq[DataGenState] = if (context.isRestored) checkpointedState.get().toList else List()
    state = stateIter match {
      case Nil => DataGenState()
      case head :: _ => head
    }

  }
}