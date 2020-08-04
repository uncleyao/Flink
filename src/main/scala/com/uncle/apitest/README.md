# FLink API
### 转换算子
* map, keyBy,reduce简单转换
* 多流转换算子，split/connect
* 自定义函数类【ex. 自定义filter】

[Transform代码](./TransformTest.scala)
### Window及WaterMark
针对EventTime处理乱序的核心
* 设置EventTime
* 指定WaterMark时间戳
* 指定时间戳
    * 非乱序
    * 乱序：需要自定义windowassigner，用于周期生成WaterMark
    
[Windowd代码](./WindowTest.scala)
### Process Function 
底层的ProcessFunction可以
* 访问Event Timestamp
* 访问WaterMark
* 注册定时事件（自定义闹钟）

[ProcessFunction代码](./ProcessFunctionTest.scala)
### 侧输出流
Process Function中的side output功能

可以实现多流且不同数据类型

[SideOutput代码](./SideOutputTest.scala)

