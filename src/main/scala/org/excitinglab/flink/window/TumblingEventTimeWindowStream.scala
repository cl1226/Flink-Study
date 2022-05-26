package org.excitinglab.flink.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 按照数据中的事件事件来发生窗口的计算
 * demo中设置窗口的滚动时间为3s
 * 数据示例：
 * 10000 hello
 * 11000 hi
 * 12000 world
 * 设置第一条数据的时间戳作为stream的timestamps
 * 第一个时间窗口为[9000,12000)
 * 9000的来源 : 10000 - (10000 - offset + size) % size
 *           = 10000 - (10000 - 0 + 3000) % 3000
 *           = 9000
 * 这么做的目的是为了防止第二条数据有可能发生时间小于第一条数据，不遗漏数据
 * 如果此时又来了一条数据
 * 11000 flink
 * 那么该条数据不会被计算
 * 因为[9000,12000)的时间窗口已经计算完毕，窗口下次的计算时间区间为[12000,15000)
 */
object TumblingEventTimeWindowStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node05", 8888)

    stream.assignAscendingTimestamps(x => {
      val splits = x.split(" ")
      splits(0).toLong
    }).flatMap(_.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .sum(1)
      .print()

    env.execute()
  }

}
