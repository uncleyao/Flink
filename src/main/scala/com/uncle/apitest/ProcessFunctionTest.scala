package com.uncle.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置EventTIme
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
     * 在此可以设置CheckPoint，以下各种例子 */
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(600000)
    // 如果传输失败就fail
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)

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
        .process(new TempIncreAlert() )

    /**
     * 监测变化值【采用状态编程】
     */
    val processedStream2 = dataStream.keyBy(_.id)
     //   .process(new TempChangeAlert() )
        .flatMap(new TempChangeAlert(10.0))

    //第三种状态编程方式，可以了解，如果不喜欢可以用上面第二种
    //flatMap+richfunction的建议方式：flatMapWithState
    val processStream3 = dataStream.keyBy(_.id)
        .flatMapWithState[(String, Double, Double), Double]{
          //如果没有状态的话，也就是没有数据来过，就将当前数据温度值存入状态
          case( input: SensorReading, None) => (List.empty,Some(input.temperature) )
            // 如果有状态，就与上次温度朱比较差值，如果大于阈值就报警
          case (input: SensorReading, lastTemp: Some[Double]) =>
            val diff = (input.temperature - lastTemp.get).abs
            if (diff > 10.0){
              ( List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
            } else ( List.empty,Some(input.temperature))
        }

    dataStream.print("input data")
    processedStream2.print("process data")
    env.execute("window test")
  }
}

/**
 * 温度上升报警
 */
class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{
  /**
   * processElement是来一个数据处理一次，但此时要保存上一次温度。所以要定义一个状态来保存数据的温度
   */
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  /** 定义一个状态，保存定时器的时间戳 */
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))


  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()
    //更新温度值
    lastTemp.update(value.temperature)

    val curTimerTs = currentTimer.value()

    //温度连续上升且没有设过定时器，则注册定时器
    if (value.temperature > preTemp && curTimerTs == 0) {
      //获取当前时间
      val timeTs = context.timerService().currentProcessingTime() + 10000L
      context.timerService().registerProcessingTimeTimer(timeTs)
      currentTimer.update(timeTs)
    }
    //如果温度下降或者第一条数据，删除定时器并清空状态
    else if (value.temperature < preTemp || preTemp == 0.0) {
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    }
  }




    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 输出报警信息
      out.collect( ctx.getCurrentKey + "温度连续上升" )
      currentTimer.clear()
    }
}

class TempChangeAlert2(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)]{
  //定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  override def processElement(value: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次温度
    val lastTemp = lastTempState.value()
    // 用当前的温度和上次求差，如果大于阈值输出报警
    val diff = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      collector.collect((value.id, lastTemp,value.temperature))
    }
    /* 之后要更新状态 */
    lastTempState.update(value.temperature)
  }
}


class TempChangeAlert(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  private var lastTempState: ValueState[Double] = _
  override def open(parameters: Configuration): Unit = {
    //初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }
  override def flatMap(in: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    val lastTemp = lastTempState.value()
    // 用当前的温度和上次求差，如果大于阈值输出报警
    val diff = (in.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect((in.id, lastTemp,in.temperature))
    }
    /* 之后要更新状态 */
    lastTempState.update(in.temperature)
  }
}