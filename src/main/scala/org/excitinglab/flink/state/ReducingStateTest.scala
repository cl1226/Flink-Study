package org.excitinglab.flink.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

/**
 * 统计每辆车的速度总和   ReducingState
 */
object ReducingStateTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-group-01")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val schema = new SimpleStringSchema()
    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](
      "test",
      schema,
      prop
    ))

    kafkaStream.map(x => {
      val arr = x.split("\t")
      (arr(1), arr(3).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String, Long), (String, Long)] {
        private var reducingState: ReducingState[Long] = _
        override def open(parameters: Configuration): Unit = {
          val desc = new ReducingStateDescriptor[Long]("Reducing", (v1: Long, v2: Long) => {
            v1 + v2
          }, createTypeInformation[Long])
          reducingState = getRuntimeContext.getReducingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          reducingState.add(value._2)
          (value._1, reducingState.get())
        }
      }).print()

    env.execute()
  }

}
