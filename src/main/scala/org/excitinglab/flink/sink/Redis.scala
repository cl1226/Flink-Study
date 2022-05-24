package org.excitinglab.flink.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import redis.clients.jedis.JedisPool

object Redis {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    val source = env.socketTextStream("node03", 8888)

    val sink = source.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    sink.addSink(new SinkFunction[(String, Int)] {
      override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
        println(value)
      }
    }).setParallelism(2)

    sink.addSink(new RichSinkFunction[(String, Int)] {
      var jedisPool: JedisPool = _

      override def open(parameters: Configuration): Unit = {
        jedisPool = new JedisPool()
      }

      override def close(): Unit = {
        jedisPool.close()
      }

      override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
        val jedis = jedisPool.getResource
        jedis.set(value._1, value._2.toString)
      }
    })

//    sink.addSink(new RedisSink[(String, Int)](
//
//    ))

    env.execute()
  }

}
