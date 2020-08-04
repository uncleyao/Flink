package com.uncle.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置EventTIme，否则默认是processing time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //指定WaterMark时间戳，默认200ms，可以设置100ms
    env.getConfig.setAutoWatermarkInterval(100L)

    val stream = env.socketTextStream("localhost", 7777)
    //在terminal开socket： nc -lk 7777
    val dataStream: DataStream[SensorReading] = stream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
    /** 指定时间戳 */
      /** 简单方法：非乱序 */
    //.assignAscendingTimestamps(_.timestamp*1000)
      /**
       * 处理乱序
       * .assignTimestampsAndWatermarks(new MyAssigner())
       */
      /**
       * 处理乱序事件，延迟一秒钟；最简单方法
       * 传入延迟时间【1s】；并覆写extractTimestamp 获取WaterMark：传入element的Timestamp
       */
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading]( Time.seconds(1) ) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000
      })

    /** 统计15秒内的最小温度，隔5秒输出一次 */
    val minTempPerWindowStream = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)  // 元组，在此也可以keyBy(0)
      /** window assigner配上window function
       * 关于timeWindow：这里的Time是Flink Windowing time；用的是SlidingEventTimeWindows，继承了WindowAssinger
       * 里面是getWindowStartWithOffset【offset东八时间是 8 】
       * timestamp - (timestamp - offset + windowSize) % windowSize
       *
       */
      .timeWindow( Time.seconds(15),Time.seconds(5)) //打开时间窗口
      // .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.hours(-8))) //北京时间window
      .reduce( (data1, data2)=>(data1._1, data1._2.min(data2._2))) //用reduce做增量聚合，前一个结果比对最新数据【比较data._2的最小值】

    minTempPerWindowStream.print("min temp")
    dataStream.print("input data")

    /** process简单例子 */
    dataStream.keyBy(_.id)
        .process(new MyProcess())


    env.execute("window test")

  }
}

/**
 * 周期性生成WaterMark的方法
 */
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{

  val bound = 60000 // 延时1分钟
  var maxTs = Long.MinValue // 观察到的最大的时间戳
  // 生成WaterMark：最大的时间戳减去延时
  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)

  // 抽取时间戳
  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp*1000) //比较出最大的时间戳
    element.timestamp*1000
  }
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, String]{
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    /**
     * context的timeservice的注册eventtime，还可以delete
     */
    context.timerService().registerEventTimeTimer(2000) //注册eventtime


  }
}