## Flink的基本操作
流处理执行环境
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
```
定义dataStream(Source阶段)
```
val dataStream = env.socketTextStream(host, port) //文本流只能并行度为1
```
Transformation阶段
```scala
val wordCountDataStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map( (_,1) )
      .keyBy(0)/** 相当于groupby，在Flink里很常用  */
      .sum(1)
```
Sink阶段
```scala
wordCountDataStream.print().setParallelism(2) 
/** 设置并行度，可以在这里【最高】，或者在提交任务时配置，默认是电脑核数 */
```
启动Executor
```
env.execute("stream word count job")
```
    


