package org.excitinglab.flink.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SessionEventTimeWindowTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node05", 8888)
      .assignAscendingTimestamps(_.split(" ")(0).toLong)


    stream.flatMap(_.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          println(s"${context.window.getStart} ---------- ${context.window.getEnd}")
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      })
      .print("res")

//    stream.flatMap(_.split(" ").tail)
//      .map((_, 1))
//      .keyBy(_._1)
//      .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//      .process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
//        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
//          println(s"${context.window.getStart} ---------- ${context.window.getEnd}")
//          for (elem <- elements) {
//            out.collect(elem)
//          }
//        }
//      })
//      .print("res")

    env.execute()

  }

}
