package com.uncle.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    /** 返回String类型的datastream */
    val streamFromFile = env.readTextFile("/Users/uncleyao/Workplace/Flink/src/main/resources/sensor.txt")

    /** map 简单的转换 */
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray = data.split(",")
      // 打包成SensorReading类
      SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })

    /** 简单转换算子
     * 返回KeyedStream[T, K] T是SensorReading， K是Java Tuple；最终返回的还是DataStream[T]
     */
    // dataStream.keyBy(0).sum(2)
    // 输出当前最新的温度+10，而时间是上一次数据时间+1
    /**
     * reduce对每次结果做一次叠加，最后返回最终结果，也是从KeyedStream开始！【x，y】分别代表上一个值和当前值
     * */
    val aggStream = dataStream.keyBy("id").reduce((x,y) => SensorReading(x.id, x.timestamp+1, y.temperature+10))


    /**
     * 多流转换算子,分流效果！！
     */
    /**
     * Split,一条流得到一个SplitStream，里面拆组
     * Select从一个SplitStream中获取成两个DataStream
     */
      /**
       * Split里输入一个函数：T => TraversableOnce[String]
       *
       * */
    val splitStream = dataStream.split(  data => {
      // Seq也是一种TraversableFactory类型
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
    // connect操作，返回ConnectedStreams[T,T2]  (在此：[ warning, low ])
    val connectedStream = warning.connect(low)
    // 这里的map很简单，传两个function，分别处理T，T2
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


    /** 函数类 */
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
 * 自定义filter function，extends之后override
 */
class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    // 样例类自带一些method
    value.id.startsWith("sensor_1")
  }
}

/**
 * RichFunction有更多的功能，比如获取上下文，并拥有一些生命周期方法
 */
class MyMapper() extends RichMapFunction[SensorReading, String]{
  override def map(in: SensorReading): String = {
    "fink"
  }
}