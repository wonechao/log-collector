package io.sugo.collect.writer.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.writer.AbstractWriter;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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
  private int currentRetryTime = 0;

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

    KafkaCallBack callBack = new KafkaCallBack();
    Map<Future<RecordMetadata>, String> messageFutureMap = new HashMap<>();
    for (String message : messages) {
      Future<RecordMetadata> future = producer.send(new ProducerRecord<Integer, String>(this.topic, message), callBack);
      messageFutureMap.put(future, message);
    }
    producer.flush();

    while (true){
      if (callBack.getError() > 0){
        Set<Long> errMsgHashSet = callBack.getErrMsgHashSet();
        List<String> retryMsg = new ArrayList<>();
        for (Future<RecordMetadata> future : messageFutureMap.keySet()) {
          FutureRecordMetadata futureRecordMetadata = (FutureRecordMetadata)future;
         long checksum = futureRecordMetadata.checksum();
          if (errMsgHashSet.contains(checksum)){
            retryMsg.add(messageFutureMap.get(future));
          }
        }

        currentRetryTime += 1;
        if (currentRetryTime >= 5){
          logger.warn("failed to send to kafka ,error:" + callBack.getError());
          currentRetryTime = 0;
          return false;
        }

        logger.warn("failed to send to kafka, error:" + callBack.getError() + " retrying...");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return write(retryMsg);
      }
      if (callBack.getSuccess() == messages.size()){
        if (logger.isDebugEnabled()) {
          long current = System.currentTimeMillis();
          logger.debug("success to send to kafka, druration(ms):" + (current - now));
        }
        currentRetryTime = 0;
        return true;
      }
    }
  }

  class KafkaCallBack implements Callback {
    private int success;
    private int error;
    private Set<Long> errMsgHashSet;

    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (metadata != null) {
        success ++;
      } else {
        error ++;
        if (errMsgHashSet == null)
          errMsgHashSet = new HashSet<>();
        errMsgHashSet.add(metadata.checksum());
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
    public Set<Long> getErrMsgHashSet() { return errMsgHashSet; }
  }

}
