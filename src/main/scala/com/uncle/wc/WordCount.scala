package com.uncle.wc

import org.apache.flink.api.scala._


// 批处理 Word Count
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取
    val inputPath = "/Users/uncleyao/Workplace/Flink/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath)

    //切分数据，然后按word分组聚合
    // 用flink不需要隐式转换？？、
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_,1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
