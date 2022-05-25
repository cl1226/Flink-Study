package org.excitinglab.flink.checkpoint

import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CheckpointTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceHost = args(0)
    val sourcePort = args(1).toInt
    val checkpointPath = args(2)

    // 每隔1000ms往数据流里插入一个barrier
    env.enableCheckpointing(1000)
    // 设置状态后端
    env.setStateBackend(new FsStateBackend(new Path(checkpointPath)))
    // 设置容许checkpoint失败的次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)
    // 设置checkpoint超时时间，默认10分钟
    env.getCheckpointConfig.setCheckpointTimeout(5 * 60 * 1000)
    // 设置checkpoint模式，默认EXACTLY_ONCE
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置checkpoint任务之间的间隔时间，防止触发太密集的flink checkpoint
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(600)
    // 设置checkpoint最大并行个数
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置flink任务失败后，checkpoint数据是否删除
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val socketStream = env.socketTextStream(sourceHost, sourcePort)

    socketStream
      .flatMap(_.split(" "))
      .filter(!"asd".equals(_))
      .map((_, 1)).uid("map")
      .keyBy(0)
      .reduce((v1, v2) => (v1._1, v1._2 + v2._2)).uid("reduce")
      .print()

    env.execute()
  }

}
