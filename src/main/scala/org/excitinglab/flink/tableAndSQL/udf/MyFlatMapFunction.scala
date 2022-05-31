package org.excitinglab.flink.tableAndSQL.udf

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

class MyFlatMapFunction extends TableFunction[Row] {

  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
  }

  def eval(val1: String, val2: String, val3: String): Unit = {
    val json = JSON.parse(val3).asInstanceOf[JSONArray]
    json.forEach(x => {
      val temp = x.asInstanceOf[JSONObject]
      val keyIter = temp.keySet().iterator()
      val row: Row = Row.withPositions(4)
      row.setField(0, val1)
      row.setField(1, val2)
      while (keyIter.hasNext) {
        val key = keyIter.next()
        row.setField(2, key)
        row.setField(3, temp.get(key))
      }
      collect(row)
    })
  }

}

object MyFlatMapFunction {
  def main(args: Array[String]): Unit = {
    val func = new MyFlatMapFunction
//    func.eval("[{\"type\":\"temperature\",\"value\":\"10\"},{\"type\":\"battery\",\"value\":\"1\"}]")
  }
}
