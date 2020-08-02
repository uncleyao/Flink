package com.uncle.sinktest


import java.util.Properties
import com.uncle.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("acks", "1")

// 输出
    val inputStream = env.addSource( new FlinkKafkaConsumer011[String]("sensor-input", new SimpleStringSchema(), properties))

    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble).toString //转换成String方便序列化输出
    })

    // sink过程，放入kafka
    dataStream.addSink( new FlinkKafkaProducer011[String]("localhost:9092","sinkTest", new SimpleStringSchema() ) )
    dataStream.print()

    env.execute("Kafka sink")
  }
}
