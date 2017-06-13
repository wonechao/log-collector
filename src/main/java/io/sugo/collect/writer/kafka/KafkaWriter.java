package io.sugo.collect.writer.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;


/**
 * Created by fengxj on 4/8/17.
 */
public class KafkaWriter extends AbstractWriter {
  private final Logger logger = LoggerFactory.getLogger(KafkaWriter.class);
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
  public boolean write(List<String> messages) {
    long now = 0;
    if (logger.isDebugEnabled()) {
      now = System.currentTimeMillis();
      logger.debug("send to kafka, message size:" + messages.size());
    }
    List<Future<RecordMetadata>> futures = new ArrayList<>();

    KafkaCallBack callBack = new KafkaCallBack();
    for (String message : messages) {
      producer.send(new ProducerRecord<Integer, String>(this.topic, message), callBack);
    }
    producer.flush();
    while (true){
      if (callBack.getError() > 0){
        logger.warn("failed to send to kafka ,error:" + callBack.getError());
        return false;
      }
      if (callBack.getSuccess() == messages.size()){
        if (logger.isDebugEnabled()) {
          long current = System.currentTimeMillis();
          logger.debug("success to send to kafka, druration(ms):" + (current - now));
        }
        return true;
      }
    }


//    if (logger.isDebugEnabled()) {
//      long current = System.currentTimeMillis();
//      logger.debug("send to kafka successfully, druration(ms):" + (current - now));
//    }
//    try {
//      for (Future<RecordMetadata> future : futures) {
//        future.get();
//      }
//    } catch (InterruptedException | ExecutionException e) {
//      logger.error("", e);
//      return false;
//    }
//    return true;
  }

  class KafkaCallBack implements Callback {
    private int success;
    private int error;
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (metadata != null) {
        success ++;
      } else {
        error ++;
        if (logger.isDebugEnabled())
          logger.error("", exception);
      }
    }
    public int getSuccess(){
      return success;
    }
    public int getError(){
      return error;
    }
  }

}
