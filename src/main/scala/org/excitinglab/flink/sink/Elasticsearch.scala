package org.excitinglab.flink.sink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.DocWriteRequest.OpType
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentFactory

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, TimeZone}

object Elasticsearch {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    val stream = env.socketTextStream("node05", 8888)

    val httpHosts: java.util.ArrayList[HttpHost] = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("node03", 9200))
    httpHosts.add(new HttpHost("node04", 9200))
    httpHosts.add(new HttpHost("node05", 9200))

    val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

    val builder = new ElasticsearchSink.Builder[String](httpHosts, new ElasticsearchSinkFunction[String] {
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        val map = new util.HashMap[String, Object]()
        map.put("message", t)
        map.put("@timestamp", new Date())

        val request = Requests.indexRequest()
          .index("flink-es-2022.06.06")
          .source(map)

        requestIndexer.add(request)

        println(s"Send success: ${map.values().toArray.mkString(" ")}")
      }
    })
    builder.setBulkFlushMaxActions(1)
    stream.addSink(builder.build()).setParallelism(2).name("es-sink").uid("es-sink")

    env.execute()
  }

}
