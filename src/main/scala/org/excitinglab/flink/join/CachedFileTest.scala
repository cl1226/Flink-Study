package org.excitinglab.flink.join

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.FileUtils

import scala.collection.mutable

/**
 * 关联维表第一种方式：cachedFile
 * 应用场景：维度表信息基本不发生改变，或者改变的频率很低
 * 先由env注册到文件系统中，任务执行的时候，taskmanager从文件系统中拉取到本地
 */
object CachedFileTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    env.registerCachedFile("E:\\workspace\\flink-study\\data\\cityInfo", "id2City")

    env.socketTextStream("node05", 8888)
      .map(_.toInt)
      .map(new RichMapFunction[Int, String] {
        private val id2CityMap = new mutable.HashMap[Int, String]
        override def open(parameters: Configuration): Unit = {
          val id2City = getRuntimeContext.getDistributedCache.getFile("id2City")
          FileUtils.readFileUtf8(id2City)
            .split("\r\n")
            .foreach(x => {
              id2CityMap.put(x.split(":")(0).toInt, x.split(":")(1))
            })
        }

        override def map(value: Int): String = {
          id2CityMap.getOrElse(value, "Not found")
        }
      }).print()

    env.execute()

  }

}
