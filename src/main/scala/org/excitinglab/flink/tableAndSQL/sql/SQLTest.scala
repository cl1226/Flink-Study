package org.excitinglab.flink.tableAndSQL.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import org.apache.flink.table.api._

object SQLTest {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

//    val tableEnv = StreamTableEnvironment.create(streamEnv)

    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .useBlinkPlanner()
      .build()

    val tableEnv = TableEnvironment.create(settings)

    val table = tableEnv.fromValues(1, 2, 3, 4, 5).as("a")

    tableEnv.registerTable("test", table)

    tableEnv.sqlQuery("select * from test").execute().print()

    tableEnv.executeSql("select * from test").print()
  }

}
