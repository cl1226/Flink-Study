package org.excitinglab.flink.window

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.util.Properties

/**
 * 读取kafka数据，每隔10s将10s内的车辆按照速度排序，同时获取最大速度及最小速度的车辆信息
 */
object TumblingProcessingTimeWindowNoKeyStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-group-01")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), prop))

    kafkaStream.map(x => {
      val splits = x.split("\t")
      (splits(1), splits(3).toLong)
    }).windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .process(new ProcessAllWindowFunction[(String, Long), String, TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
          val list = elements.toList.sortBy(_._2)
          val maxSpeedCarId = list.last._2
          val minSpeedCarId = list.head._2
          println(s"最大车速: ${maxSpeedCarId}, 车牌号: ${list.last._1}")
          println(s"最小车速: ${minSpeedCarId}, 车牌号: ${list.head._1}")
          for (elem <- list) {
            out.collect(elem.toString())
          }
        }
      }).print()

    env.execute()
  }

}
