package org.excitinglab.flink.join

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.util.{Timer, TimerTask}
import scala.collection.mutable
import scala.io.Source

/**
 * 维度表更新频繁且对维度表的实时性要求较高
 * 使用定时器，定时加载维度表的数据
 */
object SchedulerTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.socketTextStream("node05", 8888)
      .map(_.toInt)
      .map(new RichMapFunction[Int, String] {

        private val map = new mutable.HashMap[Int, String]

        override def open(parameters: Configuration): Unit = {
          load()
          val timer = new Timer(true)
          timer.schedule(new TimerTask {
            override def run(): Unit = {
              load()
            }
          }, 1000, 2000)
        }

        override def map(value: Int): String = {
          map.getOrElse(value, "Not found")
        }

        def load(): Unit = {
          val source = Source.fromFile("E:\\workspace\\flink-study\\data\\cityInfo", "UTF-8")
          source.getLines().foreach(x => {
            val str = x.split(":")
            map.put(str(0).toInt, str(1))
          })
        }
      }).print()
    env.execute()
  }

}
