# log-collector
文件日志采集工具

编译：
-----
```shell
mvn package
```

启动：
-----
```shell
bin/start.sh
```
停止：
-----

```shell
bin/stop.sh
```

配置
-----

`conf/collect.properties`

```properties
file.reader.log.dir=   #采集的文件夹
writer.kafka.topic=    #kafka的topic名称
reader.class=io.sugo.collect.reader.file.DefaultFileReader
writer.class=io.sugo.collect.writer.kafka.KafkaWriter
file.reader.log.regex=.*\.log  # 采集的文件名正则表达式
file.reader.batch.size=1000   # 数据分批发送，此配置为每个批次的大小
file.reader.scan.timerange=120 #目录过期时间，采集程序不采集超过此时间的目录，单位(minutes)
file.reader.scan.interval=10000 #目录扫描时间，单位(ms)

#kafka 客户端相关配置
kafka.bootstrap.servers=  #kafka borker地址
kafka.acks=all
kafka.retries=0
kafka.batch.size=16384000 #由file.reader.batch.size决定批次大小，所以不需要修改
kafka.linger.ms=1000000  #由file.reader.batch.size决定批次大小，所以不需要修改
kafka.buffer.memory=33554432
kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer
```
