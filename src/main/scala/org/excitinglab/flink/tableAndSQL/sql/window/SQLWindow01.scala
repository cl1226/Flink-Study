package org.excitinglab.flink.tableAndSQL.sql.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._

object SQLWindow01 {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(streamEnv)

    val stream = streamEnv.socketTextStream("node05", 8888)
      .flatMap(_.split(" "))
      .map((_, 1))

    val table = tableEnv.fromDataStream(stream).as('word, 'count)

    tableEnv.registerTable("test", table)

    tableEnv.executeSql("select word, count(1) as c from test group by word").print()

    streamEnv.execute()
  }

}
