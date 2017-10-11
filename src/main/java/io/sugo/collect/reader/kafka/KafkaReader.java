package io.sugo.collect.reader.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.WriterFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaReader extends AbstractReader{
    private final Logger logger = LoggerFactory.getLogger(KafkaReader.class);
    private static final String KAFKA_CONFIG_PREFIX = "reader.kafka.";
    private static final String READER_KAFKA_TOPICS =  "reader.kafka.topics";
    private String[] topics;
    private List<CustomKafkaConsumer> consumers = new ArrayList<CustomKafkaConsumer>();

    public KafkaReader(Configure conf, WriterFactory writerFactory) {
        super(conf, writerFactory);
        this.topics = conf.getProperty(READER_KAFKA_TOPICS).split(",");
    }

    @Override
    public void stop() {
        super.stop();
        for (CustomKafkaConsumer consumer: consumers) {
            consumer.stop();
        }
        for (CustomKafkaConsumer consumer: consumers) {
            while (!consumer.isStop()){
                try {
                    logger.info("waiting for shutdown consumer...");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    @Override
    public void read(){
        KafkaConsumer<byte[], byte[]> consumer = CustomKafkaConsumer.newConsumer(conf);
        Map<String, List<PartitionInfo>> allTopics =  consumer.listTopics();
        final AtomicInteger idx = new AtomicInteger();
        for (final String topic: topics) {
            if(!allTopics.containsKey(topic))
                continue;
            List<PartitionInfo> partitionInfos = allTopics.get(topic);

            final Map<Integer, Long> offsetMap = new HashMap<>();
            for (PartitionInfo partitionInfo: partitionInfos) {
                offsetMap.put(partitionInfo.partition(), -1l);
            }
            new Thread(new Runnable() {
                @Override
                public void run() {
                    AbstractWriter writer = null;
                    try {
                        writer = writerFactory.createWriter();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    CustomKafkaConsumer consumer = new CustomKafkaConsumer(conf, parser, writer, topic, idx.getAndIncrement(), offsetMap);
                    consumers.add(consumer);
                    consumer.work();
                }
            }).start();
        }

    }
}
