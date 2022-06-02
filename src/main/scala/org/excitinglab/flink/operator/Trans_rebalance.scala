package org.excitinglab.flink.operator

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import org.apache.flink.streaming.api.scala._

object Trans_rebalance {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(3)

    val stream = env.fromSequence(0, 100)

    val filterStream = stream.filter(_ > 10)

    val resultStream = filterStream.rebalance.map(new RichMapFunction[Long, (Int, Int)] {
      override def map(value: Long): (Int, Int) = {
        val id = getRuntimeContext.getIndexOfThisSubtask
        (id, 1)
      }
    }).keyBy(_._1).sum(1)

    resultStream.print()

    env.execute()
  }

}
