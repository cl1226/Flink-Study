package org.excitinglab.flink.connectors.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object Kafka {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(streamEnv)

    tableEnv.executeSql(
      s"""
         |CREATE TABLE KafkaTable (
         |  `event_time` TIMESTAMP(3) METADATA FROM 'timestamp',
         |  `partition` BIGINT METADATA VIRTUAL,
         |  `offset` BIGINT METADATA VIRTUAL,
         |  `user_id` BIGINT,
         |  `item_id` BIGINT,
         |  `behavior` STRING
         |) WITH (
         |  'connector' = 'kafka',
         |  'topic' = 'test',
         |  'properties.bootstrap.servers' = 'node03:9092',
         |  'properties.group.id' = 'testGroup',
         |  'scan.startup.mode' = 'latest-offset',
         |  'value.format' = 'csv'
         |)
         |""".stripMargin)

    tableEnv.executeSql("select * from KafkaTable").print()

    streamEnv.execute()

  }


}
