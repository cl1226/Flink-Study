package org.excitinglab.flink.tableAndSQL.udf

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

object AggregateFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api._

    val my_func = new MyAggregateFunction

    val stream = env.socketTextStream("node05", 8888)
      .flatMap(_.split(" "))
      .map((_, 1))

    val table = tableEnv.fromDataStream(stream).as('word, 'count)

    val result = table.groupBy('word)
      .aggregate(my_func('count))
      .select('*)

    tableEnv.toRetractStream[Row](result)
      .filter(_._1)
      .map(_._2)
      .print()

    env.execute()

  }

  class MyAggregateAccum {
    var sum: Int = _
  }

  class MyAggregateFunction extends AggregateFunction[Int, MyAggregateAccum] {
    override def getValue(accumulator: MyAggregateAccum): Int = accumulator.sum

    override def createAccumulator(): MyAggregateAccum = new MyAggregateAccum

    def accumulate(accumulator: MyAggregateAccum, value: Int): Unit = {
      accumulator.sum = accumulator.sum + value
    }
  }

}
