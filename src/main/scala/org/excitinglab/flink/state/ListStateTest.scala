package org.excitinglab.flink.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import java.text.SimpleDateFormat
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * 统计每一辆车的运行轨迹  ListState
 *
 * 1. 获取每辆车的所有信息（车牌号、卡口号、eventTime、speed）
 * 2. 根据每辆车的车牌分组
 * 3. 对每组数据中的信息按照eventTime升序排序，然后将卡口信息连接起来
 */
object ListStateTest {

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

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    kafkaStream.map(x => {
      val arr = x.split("\t")
      val time = format.parse(arr(2)).getTime
      (arr(0), arr(1), time, arr(3).toLong)
    }).keyBy(_._2)
      .map(new RichMapFunction[(String, String, Long, Long), (String, String)] {
        private var carInfos: ListState[(String, Long)] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ListStateDescriptor[(String, Long)]("list", createTypeInformation[(String, Long)])
          carInfos = getRuntimeContext.getListState(desc)
        }

        override def map(value: (String, String, Long, Long)): (String, String) = {
          carInfos.add(value._1, value._3)
          val sortList = carInfos.get().asScala.seq.toList.sortBy(_._2)
          val builder = new StringBuilder
          for (elem <- sortList) {
            builder.append(elem._1 + "\t")
          }
          (value._2, builder.toString())
        }
      }).print()

    env.execute()
  }

}
