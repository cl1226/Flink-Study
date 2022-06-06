package org.excitinglab.flink.connectors.dataset

import org.apache.flink.api.scala.ExecutionEnvironment

object Hadoop {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataset = env.readTextFile("hdfs://node02:8020/test/topn/topn.txt")

    dataset.print()
  }

}
