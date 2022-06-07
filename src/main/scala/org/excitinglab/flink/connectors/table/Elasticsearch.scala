package org.excitinglab.flink.connectors.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._

import java.text.SimpleDateFormat
import java.util.Date

object Elasticsearch {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    tableEnv.executeSql(
      s"""
         |CREATE TABLE myUserTable (
         |  `@timestamp` STRING,
         |  message STRING
         |) WITH (
         |  'connector' = 'elasticsearch-7',
         |  'hosts' = 'http://node03:9200',
         |  'index' = 'flink-es-2022.06.07'
         |)
         |""".stripMargin)

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val stream = env.socketTextStream("node05", 8888)
      .flatMap(_.split(" "))
      .map(x => {
        (format.format(new Date()), x)
      })

    val table = tableEnv.fromDataStream(stream)

//    tableEnv.registerTable("test", table)
    tableEnv.createTemporaryView("test", table)

//    tableEnv.executeSql("insert into myUserTable select * from test")
//    tableEnv.from("test").insertInto("myUserTable")

    tableEnv.createStatementSet()
      .addInsert("myUserTable", table)
      .execute()

    env.execute()
  }

}
