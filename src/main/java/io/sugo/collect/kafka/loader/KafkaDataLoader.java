package io.sugo.collect.kafka.loader;

import io.sugo.collect.Configure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


public class KafkaDataLoader {
  private static final Logger logger = LoggerFactory.getLogger(KafkaDataLoader.class);

  private  Configure conf;
  public KafkaDataLoader(){
    conf = new Configure();
  }

  public static void main(String[] args) throws Exception {
    KafkaDataLoader loader = new KafkaDataLoader();
    loader.work();
  }

  private void work() throws InterruptedException {
    String partitionStr = conf.getProperty("partitions");
    Map<String, Long> partitionMap = parsePartitions(partitionStr);
    Thread[] threads = new Thread[partitionMap.size()];
    final AtomicInteger idx = new AtomicInteger();
    final CountDownLatch latch = new CountDownLatch(threads.length);
    for (final Map.Entry<String, Long> entry : partitionMap.entrySet()) {
      threads[idx.getAndIncrement()] = new Thread(new Runnable() {
        @Override public void run() {
          KafkaDataReader reader = new KafkaDataReader(conf, idx.get(), latch, Integer.valueOf(entry.getKey()), entry.getValue());
          try {
            reader.loadToFile();
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        }
      });
    }
    for(int i = 0; i < threads.length; i++){
      threads[i].start();
    }
    latch.await();
    logger.info("shutdown successfully");
  }

  private Map<String, Long> parsePartitions(String partitionStr) {
    String[] pairs = partitionStr.split(";");
    Map<String, Long> map = new HashMap<>();
    for (int i = 0; i < pairs.length; i++) {
      String pair = pairs[i];
      String[] tmp = pair.split(":");
      map.put(tmp[0], Long.valueOf(tmp[1]));
    }
    return map;
  }
}
