package org.excitinglab.flink.source

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.{SimpleStringSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.common.serialization.StringDeserializer

import java.util.Properties

object Kafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-group-01")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].toString)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].toString)

    val schema = new SimpleStringSchema()
    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](
      "flink-topic",
      schema,
      prop
    ))

    kafkaStream.print()

    env.execute()
  }

}
