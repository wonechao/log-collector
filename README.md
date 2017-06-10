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
file.reader.log.dir=                                                               #采集的文件夹
writer.kafka.topic=                                                                #kafka的topic名称
reader.class=io.sugo.collect.reader.file.DefaultFileReader
parser.class=io.sugo.collect.parser.GrokParser
writer.class=io.sugo.collect.writer.kafka.KafkaWriter
file.reader.log.regex=.*\.log                                                      # 采集的文件名正则表达式
file.reader.batch.size=1000                                                        # 数据分批发送，此配置为每个批次的大小
file.reader.scan.timerange=120                                                     #目录过期时间，采集程序不采集超过此时间的目录，单位(minutes)
file.reader.scan.interval=10000                                                    #目录扫描时间，单位(ms)
file.reader.threadpool.size=2                                                      #reader线程池大小
file.reader.host=                                                                  #可选，如果去掉此参数，将通过 InetAddress.getLocalHost().getHostAddress()
file.reader.grok.patterns.path=conf/patterns
file.reader.grok.expr=                                                             #grok 表达式

#kafka 客户端相关配置
kafka.bootstrap.servers=                                                           #kafka borker地址
kafka.acks=1
kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer

```
