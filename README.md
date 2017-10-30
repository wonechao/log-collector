# log-collector
文件日志采集工具

## 编译：
```shell
mvn package
```

## 启动：
```shell
bin/start.sh
```
## 停止：

```shell
bin/stop.sh
```

## 验证Parser
```shell
# 在项目目录下执行以下命令行可验证当前项目conf/collect.properties中parser.class指定的parser解析情况
# 其中，${EXAMPLE_FILE_ABSOLUTE_PATH}是指用来验证的样例数据文件的绝对路径
# 例如，在"/usr/local"下有一样例数据文件"example.log"，则在进入项目根目录后，可通过执行以下命令行验证Parser运行情况
#
# bin/verify_parser.sh /usr/local/example.log
#
# 样例数据可以存放多条，以换行分隔，验证结果将在执行后逐条分别打印出[样例数据]与[解析结果]，可通过此验证Parser运行情况
bin/verify_parser.sh ${EXAMPLE_FILE_ABSOLUTE_PATH}
```

## 配置

### 文件：`conf/collect.properties`

* 参数说明

| 参数          | 默认值           | 说明  |
| ------------- |-------------   | -----|
|file.reader.log.dir |  | 采集的文件夹 |
|file.reader.log.regex |  | 采集的文件名正则表达式 |
|writer.kafka.topic|      |   kafka的topic名称 |
|reader.class  |       |reader类名，暂时只有`io.sugo.collect.reader.file.DefaultFileReader` |
|parser.class  |       |parser类名，`CSV`文件使用 `io.sugo.collect.parser.CSVParser`，`nginx`日志建议使用`io.sugo.collect.parser.GrokParser` |
|writer.class  |       | writer类名，数据写入到`kafka`使用`io.sugo.collect.writer.kafka.KafkaWriter` |
|file.reader.batch.size  |       |    数据分批发送，此配置为每个批次的大小 |
|file.reader.scan.timerange  |       |    目录过期时间，采集程序不采集超过此时间的目录，单位(minutes)名 |
|file.reader.scan.interval  |       |    目录扫描时间，单位(ms) |
|file.reader.threadpool.size  |       | reader线程池大小，一个线程负责一个采集子目录 |
|file.reader.host  |InetAddress.getLocalHost().getHostAddress()|（可选）采集机器ip|
|file.reader.grok.patterns.path  | [${user.dir}/conf/patterns](https://github.com/Datafruit/log-collector/blob/master/src/main/resources/patterns)     | （可选）grok表达式配置文件路径 |
|file.reader.grok.expr  | | grok 表达式 |
|file.reader.csv.dimPath|${user.dir}/conf/dimension| csv维度配置文件，`parser.class`为`io.sugo.collect.parser.CSVParser`时生效 |
|file.reader.csv.separator|  ,（逗号） | csv文件分隔符，空格分隔可填`space`， `parser.class`为`io.sugo.collect.parser.CSVParser`时生效 |
|gateway.api |  | 网关地址 |

* 其他参数说明

1. `kafka.`开头的配置都是用于`KafkaProducer`的构造，如`bootstrap.servers`,可以写成`kafka.bootstrap.servers`

* DEMO 1

数据：
```
[elk] [2017-05-24 16:51:23] {"a":1,"b","2"}
```
配置：
```properties
file.reader.log.dir=/data/log
writer.kafka.topic=test_topic
reader.class=io.sugo.collect.reader.file.DefaultFileReader
parser.class=io.sugo.collect.parser.GrokParser
writer.class=io.sugo.collect.writer.kafka.KafkaWriter
file.reader.log.regex=.*\.log
file.reader.batch.size=1000
file.reader.scan.timerange=120
file.reader.scan.interval=10000
file.reader.threadpool.size=2
file.reader.host=192.168.0.2
file.reader.grok.patterns.path=conf/patterns
file.reader.grok.expr=\\[%{NOTSPACE:logtype}\\] \\[%{CUSTOM_TIMESTAMP_ISO8601:logtime;date;yyyy-MM-dd HH:mm:ss}\\] %{JSON:json_base_request}

kafka.bootstrap.servers=192.168.0.3:8090
kafka.acks=1
kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer

```
* DEMO 2

```
elk 2017-05-24T16:51:23+08:00 {"a":1,"b","2"}
```
配置：
```properties
file.reader.log.dir=/data/log
writer.kafka.topic=test_topic
reader.class=io.sugo.collect.reader.file.DefaultFileReader
parser.class=io.sugo.collect.parser.CSVParser
writer.class=io.sugo.collect.writer.kafka.KafkaWriter
file.reader.log.regex=.*\.log
file.reader.batch.size=1000
file.reader.scan.timerange=120
file.reader.scan.interval=10000
file.reader.threadpool.size=2
file.reader.host=192.168.0.2
file.reader.csv.separator=space

kafka.bootstrap.servers=192.168.0.3:8090
kafka.acks=1
kafka.key.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.value.serializer=org.apache.kafka.common.serialization.StringSerializer

```

### 文件：`conf/dimension`

`parser.class`为 `io.sugo.collect.parser.CSVParser`是才需要配置

```javascript
[{
	"name": "logtype",
	"type": "string"
},
{
	"name": "logtime",
	"type": "date",
	"format": "yyyy-MM-dd'T'HH:mm:ssXXX"
},
{
	"name": "json",
	"type": "string"
},
{
	"name": "extend",
	"type": "string",
	"defaultValue": "i_am_default_value",
}]
```
* 参数说明：


 `name`：字段名称

 `type`：字段类型，`string`、`int`、`long`、`float`、`date`

 `format`：当`type`为`date`时生效，如：`yyyy-MM-dd HH:mm:ss`

 `defaultValue`：当字段值为`null`或者空字符串时，就取此值

 ### 文件：`conf/patterns`
`parser.class`为 `io.sugo.collect.parser.GrokParser`是才需要配置，配置请参考[grok](https://www.elastic.co/guide/en/logstash/current/plugins-filters-grok.html#_grok_basics)
