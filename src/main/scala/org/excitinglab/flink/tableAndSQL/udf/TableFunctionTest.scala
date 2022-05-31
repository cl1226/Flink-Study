package org.excitinglab.flink.tableAndSQL.udf

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {

  case class Message(device: String, time: String, data: String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api._

    val stream = env.socketTextStream("node05", 8888)
      .map(x => {
        val splits = x.split(" ")
        Message(splits(0), splits(1), splits(2))
      })

    val table = tableEnv.fromDataStream(stream)

    val flat_func = new MyFlatMapFunction

    val result = table.flatMap(flat_func('device, 'time, 'data)).as('device, 'time, 'type, 'value)
      .select('*)

    tableEnv.toRetractStream[Row](result)
      .filter(_._1)
      .map(_._2)
      .print()

    env.execute()

  }

}
