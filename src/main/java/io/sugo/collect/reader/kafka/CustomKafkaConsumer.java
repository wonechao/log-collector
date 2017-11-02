package io.sugo.collect.reader.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.sugo.collect.Configure;
import io.sugo.collect.parser.AbstractParser;
import io.sugo.collect.parser.IgnorableException;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class CustomKafkaConsumer{
    private static final Logger logger = LoggerFactory.getLogger(CustomKafkaConsumer.class);
    private static final String KAFKA_CONFIG_PREFIX = "reader.kafka.";
    private static final String FROM_BEGINNING = "reader.kafka.frombeginning";
    private static final String READER_KAFKA_ZOOKEEPER_SERVER =  "reader.kafka.zookeeper.server";
    private static final String READER_KAFKA_ZOOKEEPER_TIMEOUT =  "reader.kafka.zookeeper.timeout";
    private static final String READER_KAFKA_ZOOKEEPER_HOSTS_PATH =  "reader.kafka.zookeeper.hosts.path";
    private static final long POLL_TIMEOUT = 10;
    private final Configure conf;
    private final int threadNum;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;
    private final AbstractParser parser;
    private final AbstractWriter writer;
    private boolean running;
    private Map<Integer, Long> offsetMap = new HashMap<>();
    private List<String> messages;
    private final Gson gson = new GsonBuilder().create();
    private boolean stop;

    public CustomKafkaConsumer(Configure conf, AbstractParser parser, AbstractWriter writer,
                               String topic, int threadNum, Map<Integer, Long> offsetMap){
        this.conf = conf;
        this.parser = parser;
        this.writer = writer;
        this.topic = topic;
        this.threadNum = threadNum;
        this.offsetMap = offsetMap;
        consumer = newConsumer(conf);
        assignPartitions();
    }

    private static String getClusterViz(String zookeeper, int timeout, String hostsPath) {

        ZooKeeper zkClient = null;
        StringBuilder hostBuilder = new StringBuilder("");
        try {
            zkClient = new ZooKeeper(zookeeper, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                }
            });
            Gson gson = new Gson();
            List<String> ids = zkClient.getChildren(hostsPath, false);
            for (String id : ids) {
                String brokerInfo = new String(zkClient.getData(hostsPath + "/" + id, false, null));
                Map hostMap = gson.fromJson(brokerInfo, Map.class);
                hostBuilder.append(hostMap.get("host"));
                hostBuilder.append(":");
                hostBuilder.append(hostMap.get("port"));
                hostBuilder.append(",");
            }
        } catch (Exception ex) {
            logger.error("", ex);
        } finally {
            // 关闭zookeeper连接
            if (null != zkClient) {
                try {
                    zkClient.close();
                } catch (InterruptedException e) {
                    logger.error("", e);
                }
            }
            String hostString = hostBuilder.toString();
            if (0 < hostString.length()) {
                return hostString.substring(0, hostString.length() - 1);
            } else {
                return "";
            }
        }
    }

    public static KafkaConsumer<byte[], byte[]> newConsumer(Configure conf) {
        final Properties props = new Properties();
        Properties properties = conf.getProperties();
        for (Object key : properties.keySet()) {
            String keyStr = key.toString();
            if (keyStr.startsWith(KAFKA_CONFIG_PREFIX)) {
                props.put(keyStr.substring(KAFKA_CONFIG_PREFIX.length()), properties.getProperty(keyStr));
            }
        }
        String zookeeper = conf.getProperty(READER_KAFKA_ZOOKEEPER_SERVER);
        if (!zookeeper.isEmpty()) {
            int zkTimeout = Integer.parseInt(conf.getProperty(READER_KAFKA_ZOOKEEPER_TIMEOUT, "10000"));
            String zkHostsPath = conf.getProperty(READER_KAFKA_ZOOKEEPER_HOSTS_PATH);
            String brokerList = getClusterViz(zookeeper, zkTimeout, zkHostsPath);
            props.setProperty("bootstrap.servers", brokerList);
        }
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "none");
        props.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
        props.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());

        logger.info("kafka consumer properties:" + props);
        return new KafkaConsumer<>(props);

    }
    private void assignPartitions() {
        List<TopicPartition> topicPartitions = new ArrayList<>();
        Set<Integer> partitions = offsetMap.keySet();
        for (Integer partition: partitions) {
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        consumer.assign(topicPartitions);

        for (Integer partition: partitions) {
            long offset = offsetMap.get(partition);
            if (offset == -1){
                String fromBeginning = conf.getProperty(FROM_BEGINNING, "false");
                if (fromBeginning.equals("true")){
                    consumer.seekToBeginning(topicPartitions);
                }else {
                    consumer.seekToEnd(topicPartitions);
                }

                break;
            }
            offset ++;
            logger.info("Thread:" + threadNum + " Seeking topic[" + topic+ "] partition[" + partition + "] to offset[" + offset + "].");
            consumer.seek(new TopicPartition(topic, partition), offset);
        }

    }

    public void work() {
        running = true;
        Set<Integer> partitions = offsetMap.keySet();
        Map<Integer, File> offsetFiles = new HashMap<Integer, File>();
        for (Integer partition : partitions) {
            File offsetFile = new File(KafkaReader.READER_KAFKA_OFFSET_DIR + "/" + topic + "/" + partition);
            offsetFiles.put(partition, offsetFile);
        }
        int batchSize = conf.getInt(Configure.FILE_READER_BATCH_SIZE);
        ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
        while (running){
            messages = new ArrayList<>();
            records = consumer.poll(POLL_TIMEOUT);
            boolean hasrecord = false;
            for (ConsumerRecord<byte[], byte[]> record : records) {
                hasrecord = true;
                offsetMap.put(record.partition(), record.offset());
                final byte[] valueBytes = record.value();
                if (valueBytes == null) {
                    throw new RuntimeException("null value");
                }

                String message = new String(valueBytes);
                if (parser == null){
                    messages.add(message);
                }else {
                    try {
                        Map<String, Object> gmMap = parser.parse(message);
                        if (gmMap.size() > 0){
                            messages.add(gson.toJson(gmMap));
                        }
                    } catch (Exception e) {
                        if (!(e instanceof IgnorableException)){
                            StringBuffer logbuf = new StringBuffer();
                            logbuf.append(" failed to parse:").append(message);
                            logger.error(logbuf.toString(), e);
                        }
                    }
                }

                if (messages.size() >= batchSize) {
                    try {
                        write(messages);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    messages = new ArrayList<>();
                }
            }
            if (messages.size() > 0){
                try {
                    write(messages);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //todo: write offset
            if (hasrecord) {
                for (Integer partition : offsetMap.keySet()) {
                    try {
                        FileUtils.writeStringToFile(offsetFiles.get(partition), offsetMap.get(partition) + "");
                    } catch (IOException e) {
                        logger.error("failed to write offset ", e);
                    }
                }
            }

        }
        stop = true;
    }

    public void stop(){
        this.running = false;
    }

    public boolean isStop(){
        return stop;
    }

    private boolean write(List<String> messages) throws InterruptedException {
        boolean res = writer.write(messages);
        if (!res) {
            logger.warn("写入失败，retry after 1 second!!!");
            Thread.sleep(1000);
            return writer.write(messages);
        }
        return false;
    }
}
