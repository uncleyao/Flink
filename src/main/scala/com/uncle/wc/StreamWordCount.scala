package com.uncle.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    /**  1， 流处理执行环境 */
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * 最底层创建执行环境是createLocalEnvironment或者createRemoteEnvironment
     * 但如果进prod需要改，所以更多使用getExecutionEnvironment
     */
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")
    // 接收一个socket文本流
    // val dataStream = env.socketTextStream("localhost",7777)
    val dataStream = env.socketTextStream(host, port) //文本流只能并行度为1
    //对每条数据处理
    val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map( (_,1) )
      .keyBy(0)  /** 相当于groupby，在Flink里很常用  */
      .sum(1)
      // 在每个算子都可以加并行度  .setParallelism(3)


    /** 手动设置Operator Chain【disable掉Operator Chain 】
     * env.disableOperatorChaining()
     * */
    wordCountDataStream.print().setParallelism(2) /** 设置并行度，可以在这里【最高】，或者在提交任务时配置 */

    //启动Executor
    env.execute("stream word count job")

    /** nc -lk 7777 * /
     * 5> (world,1)
     * 3> (hello,1)
     * 6> (env,1)
     * 3> (hello,2)
     * 5> (world,2)
     * 6> (env,2)
     * 7> (shit,1)
     * 5> (world,3)
     * 前面是并行度
     */
  }
}
