package com.art4ul.flink.demo.window

import com.art4ul.flink.demo.entity.UserSensor
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

class TimeExtractor(maxOutOfOrderness: Time)  extends BoundedOutOfOrdernessTimestampExtractor[UserSensor](maxOutOfOrderness){
  override def extractTimestamp(element: UserSensor): Long = element.time
}
