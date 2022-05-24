package org.excitinglab.flink.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * 检测卡口车辆是否发生急加速  ValueState
 *
 * 使用ValueState，不使用变量存储 是因为ValueState能持久化到外部，下次重启能够恢复状态
 */
object ValueStateTest {
  case class CarInfo(carId: String, speed: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.socketTextStream("node03", 8888)
      .map(x => {
        val splits = x.split(" ")
        CarInfo(splits(0), splits(1).toLong)
      }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo, String] {
        var lastTempSpeed: ValueState[Long] = _
        override def open(parameters: Configuration): Unit = {
          val desc = new ValueStateDescriptor[Long]("lastSpeed", createTypeInformation[Long])
          lastTempSpeed = getRuntimeContext.getState(desc)
        }

        override def map(value: CarInfo): String = {
          val lastSpeed = lastTempSpeed.value()
          this.lastTempSpeed.update(value.speed)
          if (lastSpeed != 0 && value.speed - lastSpeed > 30) {
            "over speed: " + value.toString
          } else {
            value.carId
          }
        }
      }).print()

    env.execute()
  }

}
