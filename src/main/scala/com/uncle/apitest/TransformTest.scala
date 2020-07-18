package com.uncle.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val streamFromFile = env.readTextFile("/Users/uncleyao/Workplace/Flink/src/main/resources/sensor.txt")

    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

      /**
       * 简单转换算子
       */
    // dataStream.keyBy(0).sum(2)
    // 输出当前最新的温度+10，而时间是上一次数据时间+1
    val aggStream = dataStream.keyBy("id").reduce((x,y) => SensorReading(x.id, x.timestamp+1, y.temperature+10))


    /**
     * 多流转换算子,分流效果！！
     */
    /**
     * Split,一条流得到一个SplitStream，里面拆组
     * Select从一个SplitStream中获取成两个DataStream
     */
      // 加戳
    val splitStream = dataStream.split(  data => {
      // Seq类型？？？？？？？、
      if (data.temperature>30) Seq("high") else Seq("low")
    })
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high","low")

    /**
     * Connect把两条流包在一个流，内部依然保持各自数据独立【只能两条流】
     * CoMap同时将流的两部分进行操作，然后合并
     */
    val warning = high.map( data => (data.id, data.temperature))
    val connectedStream = warning.connect(low)
    val colMapDataStream = connectedStream.map(
      warningData => (warningData._1, warningData._2,"warning"),
      lowData => ( lowData.id, "healthy")
    )
    colMapDataStream.print()

    /**
     * UNION可以合并多条流，但必须所有数据结构都一样
     */
    val unionStream = high.union(low)
    unionStream.print("union")


    // 函数类
    dataStream.filter(new MyFilter()).print()

    high.print("high")
    low.print("low")
    all.print("all")


    /**
     * 如果多并行，返回结果会乱序,在此只是对print设置并行
     */
    dataStream.print().setParallelism(1)


    env.execute("transform test")
  }
}


/**
 * 自定义filter function
 */
class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

/**
 * RichFunction有更多的功能，比如获取上下文
 */
class MyMapper() extends RichMapFunction[SensorReading, String]{
  override def map(in: SensorReading): String = {
    "fink"
  }
}