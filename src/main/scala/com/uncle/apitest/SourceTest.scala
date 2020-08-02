package com.uncle.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import sun.java2d.pipe.SpanShapeRenderer.Simple


/** 传感器读书样例类，有Id，时间，和温度 */
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 其实底层的环境应该是createLocalEnvironment【本地执行环境】或者createRemoteEnvironment【集群执行环境，需要JobManager的IP和port】
    // 但最常用还是用get
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1，从自定义的集合中读取数据【有界】
    val stream1 = env.fromCollection( List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    // 2, 从文件或者socket【有界】
    val stream2 = env.readTextFile("/Users/uncleyao/Workplace/Flink/src/main/resources/hello.txt")

    /** 3. 从Kafka读数据【无界】 */
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("acks", "1")

    /** addSource里是一个Function，Kafka传入topic，deserialize，和property */
    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor-topic", new SimpleStringSchema(), properties))

    /**
     * Spark里对kafka的状态一致性有两种处理
     * 1，等待处理完成再改偏移量
     * 2，回到checkpoint手动更改偏移量
     */
    /**
     * Flink的区别是，他是一条条读，而且有状态；Flink可以把当前偏移量当状态保存
     * 所以在恢复的时候直接恢复当时的checkpoint
     * 不需要手动【flink的自动行】
     */
    // print,加并行度为
    stream1.print("stream1").setParallelism(1)
    stream2.print("stream2").setParallelism(1)

    stream3.print("stream3")

    env.execute("source test")
  }

}
