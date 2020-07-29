package com.uncle.apitest


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置EventTIme
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //指定WaterMark时间戳
    env.getConfig.setAutoWatermarkInterval(100L)
    //val stream  = env.readTextFile("/Users/uncleyao/Workplace/Flink/src/main/resources/sensor.txt")

    val stream = env.socketTextStream("localhost", 7777)
    //在terminal开socket： nc -lk 7777
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
    //简单方法
    //.assignAscendingTimestamps(_.timestamp*1000)
      /**
       * 处理乱序
       */
     // .assignTimestampsAndWatermarks(new MyAssigner())
      /**
       * 处理乱序事件，延迟一秒钟
       */
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000
      })
    // 复习：所有转换过程
    // 统计15秒内的最小温度，隔5秒输出一次
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow( Time.seconds(15),Time.seconds(5)) //开时间窗口
      .reduce( (data1, data2)=>(data1._1, data1._2.min(data2._2))) //用reduce做增量聚合？？？？？？？？？？？


    /**
     * 注意getWIndowStartWithOffset的源码？？？？？？
     * */
    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    dataStream.keyBy(_.id)
        .process(new MyProcess())


    env.execute("window test")

  }
}

/**
 * 周期性生成的方法
 */
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{

  val bound = 60000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp*1000)
    element.timestamp*1000
  }
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, String]{
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    /**
     * timerService可以处理多
     */
    context.timerService().registerEventTimeTimer(2000) //注册eventtime


  }
}