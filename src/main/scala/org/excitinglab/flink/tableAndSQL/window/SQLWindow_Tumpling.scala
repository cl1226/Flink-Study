package org.excitinglab.flink.tableAndSQL.window

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
 *  input.window(Tumble over 5.seconds on $"eventtime" as "w")
 *  input.window(Tumble.over("5.seconds").on("eventtime").as("w"))
 *  Tumble  滚动窗口
 *  over    定义窗口长度
 *  on      用来分组（按时间间隔）或者排序（按行数）的时间字段
 *  as      别名，必须出现在后面的groupBy中
 */
object SQLWindow_Tumpling {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    streamEnv.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(streamEnv)

    val stream = streamEnv.socketTextStream("node05", 8888)
      .map(x => {
        val split = x.split(" ")
        (split(0).toLong, split(1), 1)
      }).assignAscendingTimestamps(_._1)

    val input = tableEnv.fromDataStream(stream, $"eventtime".rowtime, $"word", $"count")

//    val table = input.window(Tumble over 5.seconds on $"eventtime" as "w")
//      .groupBy($"word", $"w")
//      .select($"word", $"w".start, $"w".end, $"w".rowtime, $"count".sum as "sum")

    val table = input.window(Tumble.over("5.seconds").on("eventtime").as("w"))
      .groupBy($"word", $"w")
      .select($"word", $"w".start, $"w".end, $"w".rowtime, $"count".sum as "sum")

    tableEnv.toRetractStream[Row](table)
      .filter(_._1)
      .map(_._2)
      .print()

//    tableEnv.createTemporaryView("test", table)

//    tableEnv.executeSql("select timestamp.eventtime, word, count(1) as c from test group by word").print()

    streamEnv.execute()
  }

}
