package org.excitinglab.flink.state

import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

/**
 * 统计每辆车的速度总和   AggregatingState
 */
object AggregatingStateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-kafka-g1")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](
      "test",
      new SimpleStringSchema(),
      prop
    ))
    kafkaStream.map(x => {
      val arr = x.split("\t")
      (arr(1), arr(3).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {
        private var aggregatingState: AggregatingState[Long, Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new AggregatingStateDescriptor[Long, Long, Long]("Aggregating", new AggregateFunction[Long, Long, Long] {
            // 初始化一个累加器
            override def createAccumulator(): Long = 0

            // 每来一条数据调用一次
            override def add(value: Long, accumulator: Long): Long = value + accumulator

            // 返回结果
            override def getResult(accumulator: Long): Long = accumulator

            // 下游算子合并各个累加器的值
            override def merge(a: Long, b: Long): Long = a + b
          }, createTypeInformation[Long])

          aggregatingState = getRuntimeContext.getAggregatingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          aggregatingState.add(value._2)
          (value._1, aggregatingState.get())
        }
      }).print()

    env.execute()
  }

}
