package io.sugo.collect.writer.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


/**
 * Created by fengxj on 4/8/17.
 */
public class KafkaWriter extends AbstractWriter {
  private final Producer<Integer, String> producer;
  private static final String KAFKA_CONFIG_PREFIX = "kafka.";
  private static final String KAFKA_TOPIC = "writer.kafka.topic";
  private final String topic;

  public KafkaWriter(Configure conf) {
    super(conf);
    this.topic = conf.getProperty(KAFKA_TOPIC);
    Properties properties = conf.getProperties();
    Properties newProperties = new Properties();
    for (Object key : properties.keySet()) {
      String keyStr = key.toString();
      if (keyStr.startsWith(KAFKA_CONFIG_PREFIX)) {
        newProperties.put(keyStr.substring(KAFKA_CONFIG_PREFIX.length()), properties.getProperty(keyStr));
      }
    }
    producer = new KafkaProducer<>(newProperties);
  }

  @Override
  public boolean write(String message) {
    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(this.topic, message));
    try {
      RecordMetadata recordMetadata = future.get();
    } catch (InterruptedException e) {
      return false;
    } catch (ExecutionException e) {
      return false;
    }
    return true;
  }

}
