package org.excitinglab.flink.tableAndSQL.udf

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

/**
 * Table function类似于FlatMap，一对多，输入一行数据输出多行数据
 */
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

  class MyFlatMapFunction extends TableFunction[Row] {

    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
    }

    def eval(val1: String, val2: String, val3: String): Unit = {
      val json = JSON.parse(val3).asInstanceOf[JSONArray]
      json.forEach(x => {
        val temp = x.asInstanceOf[JSONObject]
        val keyIter = temp.keySet().iterator()
        val row: Row = Row.withPositions(4)
        row.setField(0, val1)
        row.setField(1, val2)
        while (keyIter.hasNext) {
          val key = keyIter.next()
          row.setField(2, key)
          row.setField(3, temp.get(key))
        }
        collect(row)
      })
    }

  }

}
