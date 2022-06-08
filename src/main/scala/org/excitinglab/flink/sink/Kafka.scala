package org.excitinglab.flink.sink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.common.serialization.StringDeserializer

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import scala.util.Random

object Kafka {

  case class CarInfo(channelId: String, carId: String, eventTime: String, speed: Long) {
    override def toString: String = channelId + "," + carId + "," + eventTime + "," + speed
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SourceFunction[CarInfo] {
      var flag: Boolean = true
      val random: Random = new Random()
      val charIds = List("苏B00000", "苏B11111", "苏B22222", "苏B33333")
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      override def run(ctx: SourceFunction.SourceContext[CarInfo]): Unit = {
        while (flag) {
          val channelId: String = s"Channel-${random.nextInt(10)}"
          val carId: String = s"${charIds(random.nextInt(4))}"
          val calendar = Calendar.getInstance()
          val eventTime = simpleDateFormat.format(calendar.getTime)
          val speed = (60 + random.nextInt(40)).toLong
          val carInfo = CarInfo(channelId, carId, eventTime, speed)
          println(carInfo.toString)
          ctx.collect(carInfo)
          Thread.sleep(3000)
        }
      }

      override def cancel(): Unit = {
        this.flag = false
      }
    })

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "node03:9092,node04:9092,node05:9092")
    prop.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    prop.setProperty("value.deserializer", classOf[StringDeserializer].getName)

    stream.addSink(new FlinkKafkaProducer[CarInfo](
      "test",
      (x: CarInfo) => x.toString.getBytes(),
      prop
    ))

    env.execute()
  }
}
