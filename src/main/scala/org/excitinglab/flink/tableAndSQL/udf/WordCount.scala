package org.excitinglab.flink.tableAndSQL.udf

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object WordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val stream = env.socketTextStream("node05", 8888)

    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api._

    val table = tableEnv.fromDataStream(stream, 'words)

    val flat_func = new MyFlatMapFunction()

    val result = table.flatMap(flat_func('words)).as('word, 'count)
      .groupBy('word)
      .select('word, 'count.sum() as 'c)
    tableEnv.toRetractStream[Row](result)
      .filter(_._1)
      .map(_._2)
      .print()

    env.execute()
  }

  class MyFlatMapFunction extends TableFunction[Row] {
    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING, Types.INT)
    }

    def eval(str: String): Unit = {
      str.trim.split(" ")
        .foreach(x => {
//          val row = new Row(2)
//          row.setField(0, x)
//          row.setField(1, 1)
          val row = Row.of(x, Integer.valueOf(1))
          collect(row)
        })
    }
  }

}
