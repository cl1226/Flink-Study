package org.excitinglab.flink.tableAndSQL.window.over_window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.FieldExpression
import org.apache.flink.types.Row

/**
 * Over window聚合，会针对每个输入行，计算相邻行范围内的聚合
 * 无界Over window
 *
 *
 * 有界Over window
 * table.window(Over partitionBy $"word" orderBy $"proctime" preceding 2.rows as $"w")
 * 计算当前行及前两行 三行的聚合结果
 */
object UnboundedWindow {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(streamEnv)

    val stream = streamEnv.socketTextStream("node05", 8888)
      .map(x => {
        val splits = x.split(" ")
        (splits(0).toLong, splits(1), splits(2).toLong)
      })
      .assignAscendingTimestamps(_._1)

    val table = tableEnv.fromDataStream(stream, $"eventtime".rowtime,  $"word", $"score")

    val res = table.window(Over partitionBy $"word" orderBy $"eventtime" preceding UNBOUNDED_RANGE as $"w")
      .select($"word", $"score".sum over $"w", $"score".min over $"w")

    tableEnv.toRetractStream[Row](res)
      .filter(_._1)
      .map(_._2)
      .print()

    streamEnv.execute()
  }

}
