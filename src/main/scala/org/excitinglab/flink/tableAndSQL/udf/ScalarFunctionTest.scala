package org.excitinglab.flink.tableAndSQL.udf

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

/**
 * Scalar function 类似于Map，一对一，输入一行数据输出一行数据
 */
object ScalarFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    val stream = env.socketTextStream("node05", 8888)

    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api._

    val map_func = new MyMapFunction
    tableEnv.registerFunction("my_map", map_func)

    val table = tableEnv.fromDataStream(stream, 'words)

    val result = table.map(map_func('words)).as('words, 'words_hashcode)
    tableEnv.toRetractStream[Row](result)
      .filter(_._1)
      .map(_._2)
      .print()

    env.execute()
  }

  class MyMapFunction extends ScalarFunction {


    override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
      Types.ROW(Types.STRING, Types.STRING)
    }

    def eval(value: String): Row = {
      Row.of(value, value.hashCode.toString)
    }

  }

}
