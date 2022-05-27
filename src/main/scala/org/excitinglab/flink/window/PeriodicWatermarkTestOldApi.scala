package org.excitinglab.flink.window

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 采用官方已不推荐使用的饿AssgnerWithPeriodicWatermarks周期性设置watermark
 */
object PeriodicWatermarkTestOldApi {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setAutoWatermarkInterval(100)

    val delayTime = 3000L

    val stream = env.socketTextStream("node05", 8888)
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {

        var maxEventTime = 0L

        override def getCurrentWatermark: Watermark = {
          new Watermark(maxEventTime - delayTime)
        }

        override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
          val eventTime = element.split(" ")(0)
          maxEventTime = Math.max(maxEventTime, eventTime.toLong)
          eventTime.toLong
        }
      })

    stream.flatMap(_.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .sum(1).print()

    env.execute()
  }

}
