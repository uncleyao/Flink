package com.uncle.apitest

import akka.remote.serialization.StringSerializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputTest {
  /**
   * 判断温度，如果温度低于某个值，输出一个流
   * 如果高温输出另外一个流
   */

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置EventTIme
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("localhost", 7777)
    //在terminal开socket： nc -lk 7777
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000
      })

    val processedStream = dataStream.keyBy(_.id)
      .process(new FreezingAlert() )


    //此为主流
    processedStream.print("process data")
    //测流
    processedStream.getSideOutput( new OutputTag[String]("freezing alert")).print("alert data")
    env.execute("side output test")
  }

}

//这里String是主输出流的类型
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{
  // 此为侧输出流类型
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing alert")


  override def processElement(value: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    // 如果小于32度，输出报警
    if (value.temperature<32.0){
      context.output( alertOutput, "Freezing Alert for " + value.id)
    } else {
      out.collect(value)
    }
  }
}
