## 数据管道
要求：Flink读取Kafka Message【sensor-input】并实时写入新Kafka【sinkTest】  

启动ZooKeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties 
```
    
启动server
```
bin/kafka-server-start.sh config/server.properties
```

创建输入topic sensor-input
```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic sensor-input
```

开启Producer
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor-input
```
运行 [KafkaSink.scala](./KafkaSink.scala)

运行consumer读取数据
```
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sinkTest
```