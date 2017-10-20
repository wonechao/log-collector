package io.sugo.collect.kafka.loader;

import io.sugo.collect.Configure;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaDataReader implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(KafkaDataReader.class);

  private static final String KAFKA_CONFIG_PREFIX = "kafka.";
  private static final String KAFKA_TOPIC = "kafka.reader.topic";
  private static final long POLL_TIMEOUT = 10;
  private static final long MAX_FILE_LEN = 1000 * 1024 * 1024;
  private static final byte[] LINE = "\r\n".getBytes();

  private Configure conf;
  private String topic;
  private final KafkaConsumer<byte[], byte[]> consumer;
  private final String dataPath;
  private int fileIndex = 0;
  private File currentFile;
  private Map<Integer, Long> offsetMap = new HashMap<>();

  private CountDownLatch latch;
  private int partition;
  private long offset;
  private int threadNum;


  public KafkaDataReader(Configure conf, int threadNum, CountDownLatch latch, Integer partition, Long offset) {
    this.conf = conf;
    this.latch = latch;
    this.partition = partition;
    this.offset = offset;
    topic = conf.getProperty("topic");
    dataPath = conf.getProperty("data.path") + "/" + threadNum;
    this.threadNum = threadNum;
    consumer = newConsumer();
    assignPartitions();
  }

  private KafkaConsumer<byte[], byte[]> newConsumer() {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      final Properties props = new Properties();
      Properties properties = conf.getProperties();
      for (Object key : properties.keySet()) {
        String keyStr = key.toString();
        if (keyStr.startsWith(KAFKA_CONFIG_PREFIX)) {
          props.put(keyStr.substring(KAFKA_CONFIG_PREFIX.length()), properties.getProperty(keyStr));
        }
      }
      props.setProperty("enable.auto.commit", "false");
      props.setProperty("auto.offset.reset", "none");
      props.setProperty("key.deserializer", ByteArrayDeserializer.class.getName());
      props.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());

      logger.info("kafka consumer properties:" + props);
      return new KafkaConsumer<>(props);
    } finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private void assignPartitions() {
    List<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topic, partition));
    consumer.assign(topicPartitions);
    logger.info("Thread:" + threadNum + " Seeking partition[" + partition + "] to offset[" + offset + "].");
    consumer.seek(new TopicPartition(topic, partition), offset);
  }

  public void loadToFile() throws IOException {
    logger.info("Thread:" + threadNum + " begin to load data from kafka to file");
    logger.info("Thread:" + threadNum + " topic:" + topic + ", partition:" + partition);
    boolean reading = true;

    ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
    File path = new File(dataPath);
    if (!path.exists()) {
      path.mkdirs();
    }
    FileOutputStream fos = null;
    BufferedOutputStream bos = null;
    ObjectOutputStream oos = null;

    int recordCount = 0;
    long start = System.currentTimeMillis();
    int logIndex = 1;
    long dataSize = 0;
    long refresh = new DateTime().plusMinutes(1).getMillis();
    try {
      currentFile = new File(path, "data-" + fileIndex++);
      fos = new FileOutputStream(currentFile);
      bos = new BufferedOutputStream(fos);

      long lastRecordCount = 0;
      boolean waiting = true;
      while (reading) {
        records = consumer.poll(POLL_TIMEOUT);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          recordCount++;
          offsetMap.put(record.partition(), record.offset());
          final byte[] valueBytes = record.value();
          if (valueBytes == null) {
            throw new RuntimeException("null value");
          }
          bos.write(valueBytes, 0, valueBytes.length);
          bos.write(LINE);
          if (currentFile.length() > MAX_FILE_LEN) {
            bos.close();
            dataSize += currentFile.length();
            currentFile = new File(path, "data-" + fileIndex++);
            fos = new FileOutputStream(currentFile);
            bos = new BufferedOutputStream(fos);
          }
        }
        if (System.currentTimeMillis() > refresh) {
          logger.info("Thread:" + threadNum + " " + logIndex++ + " record count:" + recordCount + ", add record:" + (recordCount - lastRecordCount) + ", partition offsets:" + offsetMap);
          bos.flush();
          refresh = new DateTime().plusMinutes(1).getMillis();
          if (lastRecordCount == recordCount) {
            refresh = new DateTime().plusSeconds(10).getMillis();
            if (waiting) {
              waiting = false;
            } else {
              logger.info("Thread:" + threadNum + " no record for 10s, stop reading");
              break;
            }
          }
          lastRecordCount = recordCount;
        }
      }
    } finally {
      if (bos != null) {
        bos.close();
      }
      dataSize += currentFile.length();
      logger.info("Thread:" + threadNum + " total record count:" + recordCount + ", files:" + fileIndex
          + ", data size:" + String.format("%,d", dataSize)
          + ", spend time:" + ((System.currentTimeMillis() - start) / 1000) + ", partition offsets:" + offsetMap);
      if (consumer != null) {
        consumer.close();
      }
      latch.countDown();
    }
  }

  @Override public void close() throws IOException {
  }
}
