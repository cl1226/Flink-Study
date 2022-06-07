package org.excitinglab.flink.tableAndSQL.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, FieldExpression}
import org.apache.flink.table.api.Expressions.row
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row

object TableApiTest01 {

  case class Student(id: String, name: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

//    val table = tableEnv.fromValues("a", "b", "c", "d").as("col1")
//
//    table.printSchema()

    val table1 = tableEnv.fromValues(
      DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD("name", DataTypes.STRING())
      ),
      row(1, "ABC"),
      row(2L, "ABCDE")
    )

    val table2 = table1.select($"id", $"name")

//    table1.select($"*")
//    table1.filter($"id" > 10)

    val table = table1.addColumns("wuxi" as "address")

    table.printSchema()

    tableEnv.toRetractStream[Row](table)
      .filter(_._1)
      .map(_._2)
      .print()

    env.execute()
  }

}
