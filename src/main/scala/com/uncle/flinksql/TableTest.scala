package com.uncle.flinksql

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.table.api.{StreamTableEnvironment, Table, TableEnvironment}

import scala.util.parsing.json.JSON


object TableTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 3. 从Kafka读数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("acks", "1")

    val dstream: DataStream[String] = env.addSource( new FlinkKafkaConsumer011[String]("sensor-topic", new SimpleStringSchema(), properties))

    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    val ecommerceLogDStream: DataStream[EcommerceLog] = dstream.map{jsonString => JSON.parseObject(jsonString,classOf[EcommerceLog])}

    val ecommerceLogTable: Table = tableEnv.fromDataStream(ecommerceLogDStream)

    val table: Table = ecommerceLogTable.select("mid,ch").filter("ch='appstore")

    val midchDataStream: DataStream[(String, String)] = table.toAppendSteam[(String, String)]

    midchDataStream.print()
    env.execute()

  }
}
