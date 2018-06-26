package com.art4ul.flink.demo.sink

import com.art4ul.flink.demo.entity.Metric
import org.apache.flink.configuration.Configuration
import org.influxdb.InfluxDBFactory
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.influxdb.InfluxDB
import org.influxdb.dto.{Point, Query}

class InfluxSink extends RichSinkFunction[Metric] {

  var db: InfluxDB = _
  val dbName = "demo"
  val measurement = "measurement"

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    db = InfluxDBFactory.connect("http://localhost:8086", "admin", "admin")
    db.createDatabase( dbName)
    db.enableBatch( 100, 100, TimeUnit.MILLISECONDS)
  }


  override def invoke(value: Metric, context: SinkFunction.Context[_]): Unit = {
    val time = context.currentWatermark()
    val builder = Point.measurement(measurement)
      .time(value.time, TimeUnit.MILLISECONDS)
      .addField(value.name, value.value)

    val point = builder.build()

    db.write(dbName, "autogen", point)
  }

  override def close(): Unit = {
    super.close()
    db.close()
  }
}