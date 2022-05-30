package org.excitinglab.flink.tableAndSQL

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.types.Row

object CreateTable01 {

  case class Person(id: Int, name: String, score: Double)

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(streamEnv, settings)

    val stream = streamEnv.socketTextStream("node05", 8888)
      .map(x => {
        val str = x.split(" ")
        Person(str(0).toInt, str(1), str(2).toDouble)
      })

    val table = tableEnv.fromDataStream(stream, 'f_id, 'f_name, 'f_score)

    val result = table.groupBy('f_id)
      .select('f_id, 'f_score.avg())

    val ds = tableEnv.toRetractStream[Row](result)
      .filter(_._1) // 过滤是否展示修改前的数据
      .map(_._2)

    ds.print()

    streamEnv.execute("table_001")
  }

}
