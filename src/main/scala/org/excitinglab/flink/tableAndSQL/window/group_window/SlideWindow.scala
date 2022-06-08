package org.excitinglab.flink.tableAndSQL.window.group_window

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.kafka.common.serialization.StringDeserializer

import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.{Date, Properties}

/**
 *  input.window(Slide over 10.seconds every 5.seconds on $"eventTime".rowtime as "w")
 *  Slide   滑动窗口
 *  over    定义窗口长度
 *  every   滑动长度
 *  on      用来分组（按时间间隔）或者排序（按行数）的时间字段
 *  as      别名，必须出现在后面的groupBy中
 */
object SlideWindow {

  case class CarInfo(channelId: String, carId: String, eventTime: Long, speed: Long)

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    streamEnv.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(streamEnv)

    tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-group-02")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val source = streamEnv.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema, prop))
      .map(x => {
        val splits = x.split(",")
        CarInfo(splits(0), splits(1), format.parse(splits(2)).getTime, splits(3).toLong)
      }).assignAscendingTimestamps(_.eventTime)

    val input = tableEnv.fromDataStream(source, $"channelId", $"carId", $"eventTime".rowtime, $"speed")

    val table = input.window(Slide over 10.seconds every 5.seconds on $"eventTime" as "w")
      .groupBy($"carId", $"w")
      .select($"carId", $"w".start, $"w".end, $"w".rowtime, $"speed".avg as "avg")

    tableEnv.toRetractStream[Row](table)
      .filter(_._1)
      .map(_._2)
      .print("window")

    streamEnv.execute()
  }

}
