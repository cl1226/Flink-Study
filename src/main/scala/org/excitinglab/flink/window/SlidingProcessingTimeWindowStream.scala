package org.excitinglab.flink.window

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.{DateFormat, SimpleDateFormat}
import java.util.{Date, Properties}

/**
 * kafka数据源
 * 每个10s计算30分钟每辆车的平均速度
 */
object SlidingProcessingTimeWindowStream {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-group-01")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), prop))

    kafkaStream.map(x => {
      val splits = x.split("\t")
      // (carId, speed)
      (splits(1), splits(3).toLong)
    }).keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.minutes(5), Time.seconds(5)))
//      .timeWindow(Time.minutes(30), Time.seconds(10))
      .aggregate(new AggregateFunction[(String, Long), (String, Long, Long), (String, Double)] {
        override def createAccumulator(): (String, Long, Long) = ("", 0L, 0L)

        override def add(value: (String, Long), accumulator: (String, Long, Long)): (String, Long, Long) = {
          (value._1, accumulator._2 + value._2, accumulator._3 + 1)
        }

        override def getResult(accumulator: (String, Long, Long)): (String, Double) = {
          (accumulator._1, accumulator._2.toDouble/accumulator._3)
        }

        override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
          (a._1, a._2+b._2, a._3+b._3)
        }
      }, new WindowFunction[(String, Double), (String, Double), String, TimeWindow] {
        val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Double)], out: Collector[(String, Double)]): Unit = {
          println(s"${format.format(new Date(window.getStart))} --- ${format.format(new Date(window.getEnd))}")
          out.collect(input.head)
        }
      }).print()

    env.execute()
  }

}
