package org.excitinglab.flink.window

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties

/**
 * flink消费kafka数据，kafka数据示例：
 * 时间戳 msg
 * 10000 aaa
 * 设置10s的滚动窗口，设置3s的watermark，设置3s的窗口等待，设置侧输出流
 */
object PeriodicWatermarkTestNewApi {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 默认200ms
    env.getConfig.setAutoWatermarkInterval(200)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-group-01")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].toString)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].toString)

    val schema = new SimpleStringSchema()
    val kafkaSource = new FlinkKafkaConsumer[String](
      "flink-topic",
      schema,
      prop
    )

    kafkaSource.setStartFromLatest()

    kafkaSource
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[String] {
        override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
          element.split(" ").head.toLong
        }
      }))

    val kafkaStream = env.addSource(kafkaSource)

    val resStream = kafkaStream.flatMap(_.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // 窗口触发之后的3s内，如果又出现了这个窗口的数据，则这个窗口会重复计算
      .allowedLateness(Time.seconds(3))
      // 将延时特别严重的数据导致没有在13s+3s=16s内计算输出到侧数据流中
      .sideOutputLateData(new OutputTag[(String, Int)]("delayData"))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value1._2 + value2._2)
        }
      }, new WindowFunction[(String, Int), (String, Int), String, TimeWindow] {

        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          println(s"${window.getStart} --- ${window.getEnd}")
          for (elem <- input) {
            out.collect(elem)
          }
        }
      })
    resStream.print("main")
    resStream.getSideOutput(new OutputTag[(String, Int)]("delayData")).print("delayData")

    env.execute()
  }

}
