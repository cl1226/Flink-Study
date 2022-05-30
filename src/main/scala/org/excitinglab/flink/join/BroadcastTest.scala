package org.excitinglab.flink.join

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.Properties

/**
 * 维度表更新频繁，且实时性要求高
 * 通过将维度表的信息更新同步到kafka中，然后将kafka的信息变成广播流，广播到业务流的各个线程中
 */
object BroadcastTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("group.id", "flink-kafka-group01")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    val consumer = new FlinkKafkaConsumer[String](
      "configure",
      new SimpleStringSchema(),
      prop
    )
    consumer.setStartFromLatest()

    val kafkaStream = env.addSource(consumer)

    val sourceStream = env.socketTextStream("node05", 8888)

    val descriptor = new MapStateDescriptor[String, String]("configStream", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    val broadcastStream = kafkaStream.broadcast(descriptor)

    sourceStream.connect(broadcastStream)
      .process(new BroadcastProcessFunction[String, String, String] {
        override def processElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
          val map = ctx.getBroadcastState(descriptor)
          val city = map.get(value)
          city match {
            case null => out.collect("Not found!")
            case _ => out.collect(city)
          }
        }

        override def processBroadcastElement(value: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
          val map = ctx.getBroadcastState(descriptor)
          println(s"value: ${value}")
          map.put(value.split(":")(0), value.split(":")(1))
        }
      }).print()

    env.execute()
  }

}
