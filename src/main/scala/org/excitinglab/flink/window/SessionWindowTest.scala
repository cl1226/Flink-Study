package org.excitinglab.flink.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SessionWindowTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node05", 8888)

    // session的生命周期10s，窗口在10s内没有新的数据进来，窗口就会滑动（触发计算）
    stream.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .process(new ProcessAllWindowFunction[String, String, TimeWindow] {
        override def process(context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      }).print()

    env.execute()
  }

}
