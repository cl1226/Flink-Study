package org.excitinglab.flink.tableAndSQL

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object CreateTableEnvironment {

  def main(args: Array[String]): Unit = {
    // 第一种方式
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    // 第二种方式
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv2 = StreamTableEnvironment.create(env, settings)
  }

}
