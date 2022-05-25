package org.excitinglab.flink.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

object TumblingProcessingTimeWindowStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val keyedStream = env.socketTextStream("node05", 8888)
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(_._1)

    keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      },
        new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {
          val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
            println(s"${format.format(new Date(window.getStart))} --- ${format.format(new Date(window.getEnd))}")
            out.collect(input.head)
          }
        }).print()

    env.execute()
  }

}
