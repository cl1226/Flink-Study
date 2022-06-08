package org.excitinglab.flink.tableAndSQL.window.group_window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object SessionWindow {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(streamEnv)

    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api._

    val stream = streamEnv.socketTextStream("node05", 8888)
      .map(x => {
        val splits = x.split(" ")
        (splits(0).toLong, splits(1), 1)
      }).assignAscendingTimestamps(_._1)

    // session process time window
    // 在10s内没有发生事件则触发窗口的计算
//    val table = tableEnv.fromDataStream(stream, $"eventtime", $"word", $"count", $"proctime".proctime)
//
//    val table1 = table.window(Session withGap 10.seconds on $"proctime" as "w")
//      .groupBy($"word", $"w")
//      .select($"word", $"w".start, $"w".end, $"w".proctime, $"count".sum)

    // session event time window
    val table = tableEnv.fromDataStream(stream, $"eventtime".rowtime, $"word", $"count")

    val table1 = table.window(Session withGap 10.seconds on $"eventtime" as "w")
      .groupBy($"word", $"w")
      .select($"word", $"w".start, $"w".end, $"w".rowtime, $"count".sum)

    tableEnv.toRetractStream[Row](table1)
      .filter(_._1)
      .map(_._2)
      .print()

    streamEnv.execute()
  }

}
