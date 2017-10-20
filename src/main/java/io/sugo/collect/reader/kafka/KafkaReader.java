package io.sugo.collect.reader.kafka;

import io.sugo.collect.Configure;
import io.sugo.collect.reader.AbstractReader;
import io.sugo.collect.writer.AbstractWriter;
import io.sugo.collect.writer.WriterFactory;
import io.sugo.collect.writer.kafka.KafkaWriter;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaReader extends AbstractReader{
    private final Logger logger = LoggerFactory.getLogger(KafkaReader.class);
    private static final String WRITER_TO_DIFF_TOPIC = "wirter.kafka.topic.diff";
    private static final String READER_KAFKA_TOPICS =  "reader.kafka.topics";
    public static final String READER_KAFKA_OFFSET_DIR =  ".kafka_offset";
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
                Integer partition = partitionInfo.partition();
                File offsetFile = new File(KafkaReader.READER_KAFKA_OFFSET_DIR + "/" + topic + "/" + partition);
                long offset =  -1l;
                if (offsetFile.exists()){
                    try {
                        String offsetStr = FileUtils.readFileToString(offsetFile);
                        offset = Long.parseLong(offsetStr);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                logger.info("topic[" + topic +"] partition [" + partition + "] seek to offset[" + offset + "]");
                offsetMap.put(partitionInfo.partition(), offset);
            }
            new Thread(new Runnable() {
                @Override
                public void run() {
                    AbstractWriter writer = null;
                    try {
                        writer = writerFactory.createWriter();
                        if(writer instanceof KafkaWriter){
                            if (conf.getProperty(WRITER_TO_DIFF_TOPIC,"false").equals("true"))
                                ((KafkaWriter) writer).setTopic(topic + "_etl");
                        }
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
