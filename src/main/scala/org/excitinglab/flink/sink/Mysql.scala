package org.excitinglab.flink.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object Mysql {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.socketTextStream("node03", 8888)

    val res = sourceStream.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)

    res.addSink(new RichSinkFunction[(String, Int)] {

      var conn: Connection = _
      var insertStatement: PreparedStatement = _
      var updateStatement: PreparedStatement = _

      override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
        updateStatement.setInt(1, value._2)
        updateStatement.setString(2, value._1)
        updateStatement.execute()
        if (updateStatement.getUpdateCount == 0) {
          insertStatement.setString(1, value._1)
          insertStatement.setInt(2, value._2)
          insertStatement.execute()
        }
      }

      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://node02:3306/test", "root", "123456")
        insertStatement = conn.prepareStatement("insert into wc value(?,?)")
        updateStatement = conn.prepareStatement("update wc set count = ? where word = ?")
      }

      override def close(): Unit = {
        insertStatement.close()
        updateStatement.close()
        conn.close()
      }
    })

    env.execute()
  }

}
